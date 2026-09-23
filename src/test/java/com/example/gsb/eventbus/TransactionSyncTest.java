package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TransactionSyncTest {

    private DefaultEventBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    @Test
    void eventIsPublishedOnlyAfterCommit() {
        FakeTransactionContext tx = new FakeTransactionContext();
        bus = new DefaultEventBus(EventBusConfig.builder().transactionContext(tx).build());
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, received::add);

        tx.begin();
        bus.publishAfterCommit("committed-event");
        assertThat(received).isEmpty();

        tx.commit();
        assertThat(received).containsExactly("committed-event");
    }

    @Test
    void eventIsDiscardedOnRollback() {
        FakeTransactionContext tx = new FakeTransactionContext();
        bus = new DefaultEventBus(EventBusConfig.builder().transactionContext(tx).build());
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, received::add);

        tx.begin();
        bus.publishAfterCommit("rolled-back-event");
        tx.rollback();

        assertThat(received).isEmpty();
    }

    @Test
    void publishesImmediatelyWhenNoTransactionIsActive() {
        FakeTransactionContext tx = new FakeTransactionContext();
        bus = new DefaultEventBus(EventBusConfig.builder().transactionContext(tx).build());
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(String.class, received::add);

        bus.publishAfterCommit("no-tx-event");

        assertThat(received).containsExactly("no-tx-event");
    }

    private static final class FakeTransactionContext implements TransactionContext {

        private boolean active;
        private final List<TransactionSynchronization> synchronizations = new ArrayList<>();

        void begin() {
            active = true;
        }

        void commit() {
            active = false;
            synchronizations.forEach(TransactionSynchronization::afterCommit);
            synchronizations.clear();
        }

        void rollback() {
            active = false;
            synchronizations.forEach(TransactionSynchronization::afterRollback);
            synchronizations.clear();
        }

        @Override
        public boolean isTransactionActive() {
            return active;
        }

        @Override
        public void registerSynchronization(TransactionSynchronization synchronization) {
            synchronizations.add(synchronization);
        }
    }
}
