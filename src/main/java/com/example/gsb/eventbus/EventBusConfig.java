package com.example.gsb.eventbus;

import java.time.Duration;
import java.util.Objects;

public final class EventBusConfig {

    public static final EventBusConfig DEFAULT = builder().build();

    private final DispatchMode dispatchMode;
    private final int workerThreads;
    private final int queueCapacity;
    private final RejectionPolicy rejectionPolicy;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final TransactionContext transactionContext;

    private EventBusConfig(Builder builder) {
        this.dispatchMode = builder.dispatchMode;
        this.workerThreads = builder.workerThreads;
        this.queueCapacity = builder.queueCapacity;
        this.rejectionPolicy = builder.rejectionPolicy;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff;
        this.transactionContext = builder.transactionContext;
    }

    public DispatchMode dispatchMode() {
        return dispatchMode;
    }

    public int workerThreads() {
        return workerThreads;
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public RejectionPolicy rejectionPolicy() {
        return rejectionPolicy;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public Duration retryBackoff() {
        return retryBackoff;
    }

    public TransactionContext transactionContext() {
        return transactionContext;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private DispatchMode dispatchMode = DispatchMode.SYNC;
        private int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
        private int queueCapacity = 1024;
        private RejectionPolicy rejectionPolicy = RejectionPolicy.ABORT;
        private int maxRetries = 0;
        private Duration retryBackoff = Duration.ofMillis(10);
        private TransactionContext transactionContext;

        private Builder() {
        }

        /** Default dispatch mode for subscribers that do not specify one. */
        public Builder dispatchMode(DispatchMode dispatchMode) {
            this.dispatchMode = Objects.requireNonNull(dispatchMode);
            return this;
        }

        /** Number of worker threads of the internal async executor. */
        public Builder workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        /** Capacity of each worker thread's task queue. */
        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder rejectionPolicy(RejectionPolicy rejectionPolicy) {
            this.rejectionPolicy = Objects.requireNonNull(rejectionPolicy);
            return this;
        }

        /** How many times a failed delivery is retried before it becomes a dead letter. 0 disables retries. */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = Objects.requireNonNull(retryBackoff);
            return this;
        }

        public Builder transactionContext(TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        public EventBusConfig build() {
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads must be >= 1");
            }
            if (queueCapacity < 1) {
                throw new IllegalArgumentException("queueCapacity must be >= 1");
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be >= 0");
            }
            if (retryBackoff.isNegative()) {
                throw new IllegalArgumentException("retryBackoff must not be negative");
            }
            return new EventBusConfig(this);
        }
    }
}
