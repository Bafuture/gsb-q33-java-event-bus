package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DispatchModeTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    @Test
    void syncListenerRunsOnPublishingThreadBeforePublishReturns() {
        bus = new DefaultEventBus(EventBusConfig.DEFAULT);
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<Thread> listenerThread = new AtomicReference<>();
        bus.subscribe(String.class, event -> {
            listenerThread.set(Thread.currentThread());
            received.add(event);
        });

        bus.publish("hello");

        assertThat(received).containsExactly("hello");
        assertThat(listenerThread.get()).isSameAs(Thread.currentThread());
    }

    @Test
    @Timeout(10)
    void asyncPublishReturnsWhileListenerIsStillBlocked() throws Exception {
        bus = new DefaultEventBus(EventBusConfig.builder()
                .dispatchMode(DispatchMode.ASYNC)
                .workerThreads(2)
                .build());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> listenerThread = new AtomicReference<>();
        bus.subscribe(String.class, event -> {
            listenerThread.set(Thread.currentThread());
            entered.countDown();
            release.await();
        });

        bus.publish("hello", "key");
        // With sync dispatch publish() would block forever here; reaching this line proves async.
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(listenerThread.get().getName()).startsWith("event-bus-worker-");

        release.countDown();
    }

    @Test
    void dispatchModeCanBeOverriddenPerSubscription() {
        bus = new DefaultEventBus(EventBusConfig.DEFAULT); // default SYNC
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, DispatchMode.ASYNC, received::add);

        bus.publish("async-event", "key");

        Await.until(() -> received.size() == 1);
        assertThat(received).containsExactly("async-event");
    }
}
