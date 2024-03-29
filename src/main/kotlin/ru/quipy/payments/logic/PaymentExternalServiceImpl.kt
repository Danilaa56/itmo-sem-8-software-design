package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
        private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val nonBlockingOngoingWindow = NonBlockingOngoingWindow(properties.parallelRequests)
    private val rateLimiter = RateLimiter(properties.rateLimitPerSec)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newFixedThreadPool(min(200, parallelRequests))

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun canProcess(paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        val weHaveTime = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt)
        if (requestAverageProcessingTime.toMillis() > weHaveTime) {
            return false
        }
        if (nonBlockingOngoingWindow.putIntoWindow() !is NonBlockingOngoingWindow.WindowResponse.Success) {
            return false
        }

        if (!rateLimiter.tick()) {
            nonBlockingOngoingWindow.releaseWindow()
            return false
        }
        return true
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            // 18:45 -> 24:29 (344s) 349 items

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()

//            logger.info("[[$paymentId, $accountName]] submitting request into queue")
            httpClientExecutor.execute {
                processHttpRequest(paymentId, request, transactionId)
            }
        } catch (e: Exception) {
            nonBlockingOngoingWindow.releaseWindow()
        }
    }

    private fun processHttpRequest(paymentId: UUID, request: Request, transactionId: UUID?) {
        try {
//            logger.info("[[$paymentId, $accountName]] sending request")
            client.newCall(request).execute().use {
//                logger.info("[[$paymentId, $accountName]] response got")
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
                } finally {
//                    logger.info("[[$paymentId, $accountName]] response processed")
                }
            }
        } catch (e: Exception) {
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
        } finally {
            nonBlockingOngoingWindow.releaseWindow()
        }
    }
}

public fun now() = System.currentTimeMillis()

public fun passedTime(startedAt: Long) = now() - startedAt
