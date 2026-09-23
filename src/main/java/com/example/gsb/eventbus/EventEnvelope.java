package com.example.gsb.eventbus;

import java.time.Instant;

record EventEnvelope(Object event, Object aggregateKey, long sequence, Instant publishedAt) {
}
