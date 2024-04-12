package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
        properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        val circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(20f)
                .waitDurationInOpenState(Duration.ofMillis(2000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(2)
                .recordExceptions(IOException::class.java, TimeoutException::class.java)
                .build()
        val circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val callbackExecutor = Executors.newFixedThreadPool(
            16, NamedThreadFactory("callback-$serviceName-$accountName")
    )

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newFixedThreadPool(
            min(200, parallelRequests),
            NamedThreadFactory("http-$serviceName-$accountName")
    )
    private val curRequestsCount = AtomicInteger()

    private val client = OkHttpClient.Builder().run {
        protocols(listOf(Protocol.HTTP_1_1, Protocol.HTTP_2))
        dispatcher(Dispatcher(httpClientExecutor).apply {
            maxRequests = Int.MAX_VALUE // Убираем ограничение на количество одновременных запросов
            maxRequestsPerHost = Int.MAX_VALUE
        })
        build()
    }

    private val rateLimiter = RateLimiter(rateLimitPerSec)
    private val circuitBreaker = circuitBreakerRegistry.circuitBreaker("$serviceName-$accountName")

    init {
        circuitBreaker.eventPublisher
                .onError {
                    logger.error("$it")
                }
                .onStateTransition {
                    logger.error("$it")
                }
    }

    override fun canProcess(paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        val weHaveTime = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt)
        if (requestAverageProcessingTime.toMillis() > weHaveTime) {
            return false
        }
        val timeToProcessExistingQueue = curRequestsCount.toLong() / min(parallelRequests.toDouble() / requestAverageProcessingTime.toMillis(), rateLimitPerSec.toDouble())
        logger.info("[[paymentId: {}]] we have {} ms, queue will take {} ms, queue size {}", paymentId, weHaveTime, timeToProcessExistingQueue, curRequestsCount.get())
        if (timeToProcessExistingQueue + requestAverageProcessingTime.toMillis() > weHaveTime) {
            return false
        }
        if (!circuitBreaker.tryAcquirePermission()) {
            return false
        }
        return true
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить, что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()

            submitHttpRequest(paymentId, request, transactionId)
        } catch (e: Exception) {
            logger.error("failed to submit payment", e)
        }
    }

    private fun submitHttpRequest(paymentId: UUID, request: Request, transactionId: UUID?) {
        curRequestsCount.incrementAndGet();
        logger.info("[$paymentId, $accountName]: submitting request into queue")
        try {
            httpClientExecutor.submit {
                rateLimiter.tickBlocking()
                val start = now()
                try {
                    val response = client.newCall(request).execute()
                    circuitBreaker.onSuccess(now() - start, TimeUnit.MILLISECONDS)
                    callbackExecutor.submit {
                        processResponse(response, transactionId, paymentId)
                    }
                } catch (e: Exception) {
                    circuitBreaker.onError(now() - start, TimeUnit.MILLISECONDS, e)
                    callbackExecutor.submit {
                        fail(e, paymentId, transactionId)
                    }
                } finally {
                    curRequestsCount.decrementAndGet()
                }
            }
        } catch (e: Exception) {
            curRequestsCount.decrementAndGet()
            callbackExecutor.submit {
                fail(e, paymentId, transactionId)
            }
        }
    }

    private fun fail(e: Exception, paymentId: UUID, transactionId: UUID?) {
        when (e) {
            is SocketTimeoutException -> {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    private fun processResponse(it: Response, transactionId: UUID?, paymentId: UUID) =
            try {
                val body = try {
                    mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.code}, reason: ${it.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            } catch (e: Exception) {
                logger.error("Unexpected exception: ", e)
            } finally {
                logger.info("[[$paymentId, $accountName]] response processed")
            }
}

public fun now() = System.currentTimeMillis()

public fun passedTime(startedAt: Long) = now() - startedAt
