package com.example.gsb.eventbus;

/**
 * SPI bridging the event bus to a transaction manager. Implementations may wrap
 * Spring's {@code TransactionSynchronizationManager} or any custom transaction boundary.
 */
public interface TransactionContext {

    boolean isTransactionActive();

    void registerSynchronization(TransactionSynchronization synchronization);
}
