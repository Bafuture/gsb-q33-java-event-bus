package com.example.gsb.eventbus;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public final class DefaultEventBus implements EventBus {

    private static final Logger LOG = System.getLogger(DefaultEventBus.class.getName());

    private final EventBusConfig config;
    private final EventExecutor executor;
    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Queue<DeliveryFailure> failures = new ConcurrentLinkedQueue<>();
    private final InMemoryDeadLetterQueue deadLetterQueue;
    private final AtomicLong sequence = new AtomicLong();
    private volatile boolean closed;

    public DefaultEventBus(EventBusConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = new EventExecutor(config.workerThreads(), config.queueCapacity(), config.rejectionPolicy());
        this.deadLetterQueue = new InMemoryDeadLetterQueue(this::publish);
    }

    @Override
    public <T> Subscription subscribe(Class<T> eventType, EventListener<? super T> listener) {
        return subscribe(eventType, config.dispatchMode(), listener);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Subscription subscribe(Class<T> eventType, DispatchMode mode, EventListener<? super T> listener) {
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(listener, "listener");
        Subscriber subscriber = new Subscriber(eventType, mode, (EventListener<Object>) listener, describe(listener));
        subscribers.add(subscriber);
        return () -> subscribers.remove(subscriber);
    }

    @Override
    public void publish(Object event) {
        publish(event, null);
    }

    @Override
    public void publish(Object event, Object aggregateKey) {
        Objects.requireNonNull(event, "event");
        ensureOpen();
        EventEnvelope envelope = new EventEnvelope(event, aggregateKey, sequence.getAndIncrement(), Instant.now());
        for (Subscriber subscriber : subscribers) {
            if (subscriber.eventType().isAssignableFrom(event.getClass())) {
                dispatch(subscriber, envelope);
            }
        }
    }

    @Override
    public void publishAfterCommit(Object event) {
        publishAfterCommit(event, null);
    }

    @Override
    public void publishAfterCommit(Object event, Object aggregateKey) {
        Objects.requireNonNull(event, "event");
        TransactionContext context = config.transactionContext();
        if (context != null && context.isTransactionActive()) {
            context.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    publish(event, aggregateKey);
                }
            });
        } else {
            publish(event, aggregateKey);
        }
    }

    @Override
    public DeadLetterQueue deadLetters() {
        return deadLetterQueue;
    }

    @Override
    public List<DeliveryFailure> failures() {
        return List.copyOf(new ArrayList<>(failures));
    }

    @Override
    public void close() {
        closed = true;
        executor.close();
    }

    private void dispatch(Subscriber subscriber, EventEnvelope envelope) {
        if (subscriber.mode() == DispatchMode.SYNC) {
            deliver(subscriber, envelope);
        } else {
            executor.execute(envelope.aggregateKey(), () -> deliver(subscriber, envelope));
        }
    }

    private void deliver(Subscriber subscriber, EventEnvelope envelope) {
        int attempt = 0;
        while (true) {
            try {
                subscriber.listener().onEvent(envelope.event());
                return;
            } catch (Throwable error) {
                attempt++;
                DeliveryFailure failure = new DeliveryFailure(envelope.event(), envelope.aggregateKey(),
                        subscriber.id(), error, attempt, Instant.now());
                failures.add(failure);
                LOG.log(Level.WARNING, "delivery failed (attempt " + attempt + ") for subscriber "
                        + subscriber.id(), error);
                if (attempt > config.maxRetries()) {
                    deadLetterQueue.add(new DeadLetter(UUID.randomUUID(), envelope.event(),
                            envelope.aggregateKey(), subscriber.id(), error, attempt, Instant.now()));
                    return;
                }
                sleepBeforeRetry();
            }
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(config.retryBackoff().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("event bus is closed");
        }
    }

    private static String describe(Object listener) {
        return listener.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(listener));
    }

    private record Subscriber(Class<?> eventType, DispatchMode mode, EventListener<Object> listener, String id) {
    }
}
