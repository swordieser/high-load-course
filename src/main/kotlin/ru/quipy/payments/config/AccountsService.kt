package ru.quipy.payments.config

import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.ExternalServiceProperties
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Service
class AccountService {
    val accounts = ExternalServicesConfig.properties.map { Account(it) }

    fun getAvailableAccountOrNull(paymentStartedAt: Long): Account? {
        return accounts.filter {
            it.isAvailable(paymentStartedAt)
        }.minByOrNull {
            it.properties.callCost
        }
    }
}

class Account(
    val properties: ExternalServiceProperties
) {
    private val paymentOperationTimeout = Duration.ofSeconds(80)
    private val rateLimiter = RateLimiter(properties.rateLimitPerSec)

    val httpExecutor = Executors.newFixedThreadPool(
        properties.parallelRequests,
        NamedThreadFactory("${properties.accountName}-http-executor")
    )
    val executor = Executors.newFixedThreadPool(128, NamedThreadFactory("${properties.accountName}-account-executor"))

    val responsePool = Executors.newFixedThreadPool(
        128, NamedThreadFactory("${properties.accountName}-payment-response")
    )


    val httpClient = OkHttpClient.Builder()
        .dispatcher(Dispatcher(httpExecutor).apply { maxRequests = properties.parallelRequests })
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(properties.parallelRequests, paymentOperationTimeout.seconds, TimeUnit.SECONDS))
        .build()

    fun isAvailable(paymentStartedAt: Long): Boolean {
        return rateLimiter.tick() && Duration.ofSeconds((now() - paymentStartedAt) / 1000) < paymentOperationTimeout
    }


    fun now() = System.currentTimeMillis()
}