package com.example.gsb.eventbus;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-size executor with one bounded queue per worker thread. Tasks carrying the
 * same aggregate key are always routed to the same worker, which gives per-key FIFO
 * ordering while different keys run in parallel.
 */
final class EventExecutor implements AutoCloseable {

    private static final Logger LOG = System.getLogger(EventExecutor.class.getName());

    private final List<BlockingQueue<Runnable>> queues;
    private final List<Thread> workers;
    private final RejectionPolicy rejectionPolicy;
    private final AtomicLong roundRobin = new AtomicLong();
    private final int workerCount;

    EventExecutor(int workerCount, int queueCapacity, RejectionPolicy rejectionPolicy) {
        this.workerCount = workerCount;
        this.rejectionPolicy = rejectionPolicy;
        this.queues = new ArrayList<>(workerCount);
        this.workers = new ArrayList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueCapacity);
            queues.add(queue);
            Thread worker = new Thread(() -> runWorker(queue), "event-bus-worker-" + i);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }
    }

    void execute(Object aggregateKey, Runnable task) {
        int index = route(aggregateKey);
        BlockingQueue<Runnable> queue = queues.get(index);
        switch (rejectionPolicy) {
            case ABORT -> {
                if (!queue.offer(task)) {
                    throw new EventRejectedException(
                            "event queue " + index + " is full (aggregateKey=" + aggregateKey + ")");
                }
            }
            case DISCARD -> queue.offer(task);
            case CALLER_RUNS -> {
                if (!queue.offer(task)) {
                    task.run();
                }
            }
            default -> throw new IllegalStateException("unexpected policy: " + rejectionPolicy);
        }
    }

    private int route(Object aggregateKey) {
        if (aggregateKey == null) {
            return (int) Math.floorMod(roundRobin.getAndIncrement(), workerCount);
        }
        return Math.floorMod(aggregateKey.hashCode(), workerCount);
    }

    private void runWorker(BlockingQueue<Runnable> queue) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Runnable task = queue.take();
                try {
                    task.run();
                } catch (Throwable error) {
                    LOG.log(Level.ERROR, "uncaught error in event task", error);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        for (Thread worker : workers) {
            worker.interrupt();
        }
        for (Thread worker : workers) {
            try {
                worker.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
