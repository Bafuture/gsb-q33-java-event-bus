package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ExceptionIsolationTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    @Test
    void failingSubscriberDoesNotAffectOtherSubscribers_sync() {
        bus = new DefaultEventBus(EventBusConfig.DEFAULT);
        bus.subscribe(String.class, event -> {
            throw new IllegalStateException("boom");
        });
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, received::add);

        bus.publish("hello");

        assertThat(received).containsExactly("hello");
        assertThat(bus.failures()).hasSize(1);
        assertThat(bus.failures().get(0).error()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failingSubscriberDoesNotAffectOtherSubscribers_async() {
        bus = new DefaultEventBus(EventBusConfig.builder()
                .dispatchMode(DispatchMode.ASYNC)
                .workerThreads(2)
                .build());
        bus.subscribe(String.class, event -> {
            throw new IllegalStateException("boom");
        });
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, received::add);

        bus.publish("hello", "key");

        Await.until(() -> received.size() == 1 && bus.failures().size() == 1);
        assertThat(received).containsExactly("hello");
    }

    @Test
    void failedDeliveryIsRetriedUntilItSucceeds() {
        bus = new DefaultEventBus(EventBusConfig.builder()
                .maxRetries(3)
                .retryBackoff(java.time.Duration.ofMillis(1))
                .build());
        AtomicInteger attempts = new AtomicInteger();
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, event -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("not yet");
            }
            received.add(event);
        });

        bus.publish("hello");

        assertThat(received).containsExactly("hello");
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(bus.failures()).hasSize(2);
        assertThat(bus.deadLetters().size()).isZero();
    }

    @Test
    void deliveryWithoutRetryGoesStraightToDeadLetter() {
        bus = new DefaultEventBus(EventBusConfig.DEFAULT); // maxRetries = 0
        bus.subscribe(String.class, event -> {
            throw new IllegalStateException("always fails");
        });

        bus.publish("hello");

        assertThat(bus.failures()).hasSize(1);
        assertThat(bus.deadLetters().size()).isEqualTo(1);
    }
}
