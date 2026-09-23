package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class OrderingTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    private DefaultEventBus asyncBus(int workers) {
        return new DefaultEventBus(EventBusConfig.builder()
                .dispatchMode(DispatchMode.ASYNC)
                .workerThreads(workers)
                .queueCapacity(4096)
                .build());
    }

    @Test
    void eventsWithSameAggregateKeyAreConsumedInPublishOrder() {
        bus = asyncBus(4);
        List<Integer> received = new CopyOnWriteArrayList<>();
        bus.subscribe(Integer.class, received::add);

        int count = 500;
        for (int i = 0; i < count; i++) {
            bus.publish(i, "aggregate-1");
        }

        Await.until(() -> received.size() == count);
        assertThat(received).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, count).boxed().toList());
    }

    @Test
    void orderingIsMaintainedPerKeyWhenManyKeysAreInterleaved() {
        bus = asyncBus(4);
        Map<String, ConcurrentLinkedQueue<Integer>> receivedPerKey = new ConcurrentHashMap<>();
        AtomicInteger total = new AtomicInteger();
        bus.subscribe(SequencedEvent.class, event -> {
            receivedPerKey.computeIfAbsent(event.key(), k -> new ConcurrentLinkedQueue<>()).add(event.sequence());
            total.incrementAndGet();
        });

        int keys = 8;
        int perKey = 100;
        for (int i = 0; i < perKey; i++) {
            for (int k = 0; k < keys; k++) {
                String key = "agg-" + k;
                bus.publish(new SequencedEvent(key, i), key);
            }
        }

        Await.until(() -> total.get() == keys * perKey);
        assertThat(receivedPerKey).hasSize(keys);
        receivedPerKey.forEach((key, received) -> assertThat(received)
                .as("order for key %s", key)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, perKey).boxed().toList()));
    }

    @Test
    void differentAggregateKeysAreProcessedInParallel() throws Exception {
        int workers = 2;
        bus = asyncBus(workers);
        String keyA = keyForSlot(0, workers);
        String keyB = keyForSlot(1, workers);

        CountDownLatch bothEntered = new CountDownLatch(2);
        AtomicInteger passedRendezvous = new AtomicInteger();
        bus.subscribe(String.class, event -> {
            bothEntered.countDown();
            // Only passes if the other key's listener is running concurrently.
            if (bothEntered.await(5, TimeUnit.SECONDS)) {
                passedRendezvous.incrementAndGet();
            }
        });

        bus.publish("a", keyA);
        bus.publish("b", keyB);

        Await.until(() -> passedRendezvous.get() == 2);
    }

    private static String keyForSlot(int slot, int workers) {
        for (int i = 0; ; i++) {
            String candidate = "key-" + i;
            if (Math.floorMod(candidate.hashCode(), workers) == slot) {
                return candidate;
            }
        }
    }

    private record SequencedEvent(String key, int sequence) {
    }
}
