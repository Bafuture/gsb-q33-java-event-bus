package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ExecutorRejectionTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    private DefaultEventBus asyncBus(RejectionPolicy policy) {
        return new DefaultEventBus(EventBusConfig.builder()
                .dispatchMode(DispatchMode.ASYNC)
                .workerThreads(1)
                .queueCapacity(1)
                .rejectionPolicy(policy)
                .build());
    }

    @Test
    void abortPolicyThrowsWhenQueueIsFull() throws Exception {
        bus = asyncBus(RejectionPolicy.ABORT);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        bus.subscribe(String.class, event -> {
            entered.countDown();
            release.await();
        });

        bus.publish("first", "k");
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        bus.publish("second", "k"); // fills the single queue slot

        assertThatThrownBy(() -> bus.publish("third", "k"))
                .isInstanceOf(EventRejectedException.class);

        release.countDown();
    }

    @Test
    void callerRunsPolicyExecutesOnPublishingThreadWhenQueueIsFull() throws Exception {
        bus = asyncBus(RejectionPolicy.CALLER_RUNS);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> callerThread = new AtomicReference<>();
        bus.subscribe(String.class, event -> {
            if (event.equals("third")) {
                callerThread.set(Thread.currentThread().getName());
                return;
            }
            entered.countDown();
            release.await();
        });

        bus.publish("first", "k");
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        bus.publish("second", "k");
        bus.publish("third", "k"); // queue full -> runs on this thread

        assertThat(callerThread.get()).isEqualTo(Thread.currentThread().getName());

        release.countDown();
    }

    @Test
    void discardPolicySilentlyDropsWhenQueueIsFull() throws Exception {
        bus = asyncBus(RejectionPolicy.DISCARD);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, event -> {
            received.add(event);
            entered.countDown();
            release.await();
        });

        bus.publish("first", "k");
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        bus.publish("second", "k");
        bus.publish("third", "k"); // dropped silently

        release.countDown();
        Await.until(() -> received.size() == 2);
        Thread.sleep(100); // give a dropped task a chance to (incorrectly) appear
        assertThat(received).containsExactly("first", "second");
    }
}
