package com.example.gsb.eventbus;

import java.time.Instant;

/**
 * Recorded for every failed delivery attempt (including attempts that are later
 * retried successfully).
 */
public record DeliveryFailure(
        Object event,
        Object aggregateKey,
        String subscriberId,
        Throwable error,
        int attempt,
        Instant failedAt) {
}
