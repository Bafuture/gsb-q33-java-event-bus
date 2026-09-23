package com.example.gsb.eventbus;

public interface TransactionSynchronization {

    default void afterCommit() {
    }

    default void afterRollback() {
    }
}
