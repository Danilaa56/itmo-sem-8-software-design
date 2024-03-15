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
import java.util.concurrent.atomic.AtomicInteger


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

    private val curRequestsCount = AtomicInteger()
    private val nonBlockingOngoingWindow = NonBlockingOngoingWindow(properties.parallelRequests)
    private val rateLimiter = RateLimiter(properties.rateLimitPerSec)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        val weHaveTime = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt) - 5000
        if (requestAverageProcessingTime.toMillis() > weHaveTime) {
            return false
        }
        if (nonBlockingOngoingWindow.putIntoWindow() !is NonBlockingOngoingWindow.WindowResponse.Success) {
            return false;
        }
        try {
            if (!rateLimiter.tick()) {
                return false
            }
            submitPaymentRequestActual(paymentId, amount, paymentStartedAt)
            return true
        } finally {
            nonBlockingOngoingWindow.releaseWindow()
        }
//        while (true) {
//            val cur = curRequestsCount.get()
//            if (cur >= parallelRequests) {
//                return false
//            }
//
//            val weHaveTime = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt) - 10000
//            if (requestAverageProcessingTime.toMillis() > weHaveTime) {
//                return false
//            }
////            if (requestAverageProcessingTime
////                            .multipliedBy(cur.toLong())
////                            .dividedBy(rateLimitPerSec.toLong())
////                            .plus(requestAverageProcessingTime.multipliedBy(2))
////                            .plusMillis(now())
////                            .toMillis() > paymentOperationTimeout.plusMillis(paymentStartedAt).toMillis()) {
////                return false
////            }
//
//
//            // Accept submission
//            if (curRequestsCount.compareAndSet(cur, cur + 1)) {
//                break
//            }
//        }
//        try {
//            submitPaymentRequestActual(paymentId, amount, paymentStartedAt)
//            return true
//        } finally {
//            curRequestsCount.decrementAndGet()
//        }
    }

    fun submitPaymentRequestActual(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
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
        }
    }
}

public fun now() = System.currentTimeMillis()
