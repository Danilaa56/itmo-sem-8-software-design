package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig {
    companion object {
        const val ACCOUNT_1 = "PAYMENT_SERVICE_ACCOUNT_1"
        const val ACCOUNT_2 = "PAYMENT_SERVICE_ACCOUNT_2"
        const val ACCOUNT_3 = "PAYMENT_SERVICE_ACCOUNT_3"
        const val ACCOUNT_4 = "PAYMENT_SERVICE_ACCOUNT_4"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        // Throughput sum 113.8

        // Throughput 100
        private val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            parallelRequests = 1000,
            rateLimitPerSec = 100,
            request95thPercentileProcessingTime = Duration.ofMillis(1000),
        )

        // Throughput 10
        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 130,
            rateLimitPerSec = 50,
            request95thPercentileProcessingTime = Duration.ofMillis(5_000),
        )

        // Throughput 3
        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 35,
            rateLimitPerSec = 10,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        // Throughput 0.8
        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 15,
            rateLimitPerSec = 10,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )
    }

    @Bean(ACCOUNT_1)
    fun account1() =
            PaymentExternalServiceImpl(
                    accountProps_1,
            )

    @Bean(ACCOUNT_2)
    fun account2() =
            PaymentExternalServiceImpl(
                    accountProps_2,
            )

    @Bean(ACCOUNT_3)
    fun account3() =
            PaymentExternalServiceImpl(
                    accountProps_3,
            )

    @Bean(ACCOUNT_4)
    fun account4() =
            PaymentExternalServiceImpl(
                    accountProps_4,
            )
}
