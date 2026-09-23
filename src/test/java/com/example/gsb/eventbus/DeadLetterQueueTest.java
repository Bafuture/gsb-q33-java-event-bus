package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class DeadLetterQueueTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    @Test
    void retriesExhaustedMovesEventToDeadLetterQueue() {
        bus = new DefaultEventBus(EventBusConfig.builder()
                .maxRetries(2)
                .retryBackoff(java.time.Duration.ofMillis(1))
                .build());
        bus.subscribe(String.class, event -> {
            throw new IllegalStateException("always fails");
        });

        bus.publish("doomed", "agg-1");

        assertThat(bus.failures()).hasSize(3); // initial attempt + 2 retries
        assertThat(bus.deadLetters().list()).hasSize(1);
        DeadLetter letter = bus.deadLetters().list().get(0);
        assertThat(letter.event()).isEqualTo("doomed");
        assertThat(letter.aggregateKey()).isEqualTo("agg-1");
        assertThat(letter.attempts()).isEqualTo(3);
        assertThat(letter.error()).isInstanceOf(IllegalStateException.class);
        assertThat(bus.deadLetters().find(letter.id())).contains(letter);
    }

    @Test
    void deadLetterCanBeRepublishedManually() {
        bus = new DefaultEventBus(EventBusConfig.DEFAULT);
        AtomicBoolean failing = new AtomicBoolean(true);
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, event -> {
            if (failing.get()) {
                throw new IllegalStateException("boom");
            }
            received.add(event);
        });

        bus.publish("evt", "agg-1");
        assertThat(bus.deadLetters().size()).isEqualTo(1);
        DeadLetter letter = bus.deadLetters().list().get(0);

        failing.set(false);
        assertThat(bus.deadLetters().republish(letter.id())).isTrue();

        assertThat(received).containsExactly("evt");
        assertThat(bus.deadLetters().size()).isZero();
        assertThat(bus.deadLetters().republish(letter.id())).isFalse();
    }
}
