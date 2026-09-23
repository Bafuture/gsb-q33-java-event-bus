package com.example.gsb.eventbus;

import java.util.List;

public interface EventBus extends AutoCloseable {

    /**
     * Subscribes to events whose runtime type is assignable to {@code eventType}
     * (i.e. the type itself, its subtypes and interface implementations).
     */
    <T> Subscription subscribe(Class<T> eventType, EventListener<? super T> listener);

    <T> Subscription subscribe(Class<T> eventType, DispatchMode mode, EventListener<? super T> listener);

    /** Publishes without an aggregate key; no ordering guarantees relative to other keyless events. */
    void publish(Object event);

    /**
     * Publishes with an aggregate key. Events with the same key are delivered to each
     * subscriber in publish order; events with different keys may be processed in parallel.
     */
    void publish(Object event, Object aggregateKey);

    void publishAfterCommit(Object event);

    /**
     * If a transaction is active (via the configured {@link TransactionContext}), the event is
     * only published after the transaction commits; a rollback discards it. Without an active
     * transaction the event is published immediately.
     */
    void publishAfterCommit(Object event, Object aggregateKey);

    DeadLetterQueue deadLetters();

    /** Snapshot of all recorded delivery failures. */
    List<DeliveryFailure> failures();

    @Override
    void close();
}
