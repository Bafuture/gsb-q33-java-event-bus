package com.example.gsb.eventbus;

import java.time.Instant;
import java.util.UUID;

/**
 * An event whose delivery to a subscriber failed permanently (retries exhausted).
 */
public record DeadLetter(
        UUID id,
        Object event,
        Object aggregateKey,
        String subscriberId,
        Throwable error,
        int attempts,
        Instant deadAt) {
}
