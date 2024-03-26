package ru.quipy.payments.config

import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.payments.logic.ExternalServiceProperties
import java.util.concurrent.atomic.AtomicLong
import java.time.Duration

@Service
class AccountService {
    val accounts = ExternalServicesConfig.properties.map { Account(it) }

    fun getAvailableAccountOrNull(): Account? {
        return accounts.filter {
            it.isAvailable()
        }.minByOrNull {
            it.properties.callCost
        }?.apply {
            this.registerRequest()
        }
    }
}

class Account(
    val properties: ExternalServiceProperties
) {
    private val lastRequestTime = AtomicLong(System.currentTimeMillis())
    private val requestsCounter = AtomicLong(0)
    private val window = NonBlockingOngoingWindow(properties.parallelRequests)

    fun isAvailable(): Boolean {
        val now = System.currentTimeMillis()
        val put = window.putIntoWindow()

        if (!put) {
            return false
        }

        if (Duration.ofMillis(now - lastRequestTime.get()) > Duration.ofSeconds(1)) {
            lastRequestTime.set(now)
            requestsCounter.set(0)
        }

        return requestsCounter.get() < properties.rateLimitPerSec
    }

    fun registerRequest() {
        requestsCounter.incrementAndGet()
    }

    fun releaseWindow() {
        window.releaseWindow()
    }
}