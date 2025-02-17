package io;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A background writer that periodically flushes queued data using a provided flush strategy.
 * Thread-safe and supports graceful shutdown.
 *
 * @param <T> Type of data to be written
 * @see FlushStrategy
 * @see LinkedBlockingQueue
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * try (BackgroundWriter<String> writer = new BackgroundWriter<>(
 *      batch -> System.out.println("Flushing: " + batch),
 *      Duration.ofSeconds(1),
 *      6,
 *      10
 *  )) {
 *      writer.start();
 *      writer.write("Item 1");
 *      writer.write("Item 2");
 * } catch (Exception e) {
 *      e.printStackTrace();
 * }
 * </pre>
 */
public class BackgroundWriter<T> implements AutoCloseable {
    private final BlockingQueue<T> queue;
    private final FlushStrategy<T> flushStrategy;
    private final Duration flushInterval;
    private final int batchSize;
    private final AtomicBoolean running;
    private Thread workerThread;

    /**
     * Strategy for flushing batched data.
     */
    @FunctionalInterface
    public interface FlushStrategy<T> {
        /**
         * Flushes the provided batch of data.
         * This method will be called periodically from a background thread.
         *
         * @param batch Data to flush, will never be null or empty
         * @throws Exception if flush fails
         */
        void flush(List<T> batch) throws Exception;
    }

    /**
     * Creates a new background writer.
     *
     * @param flushStrategy Strategy for flushing data
     * @param flushInterval How often to flush data
     * @param batchSize Maximum items to accumulate before forcing a flush
     * @param queueCapacity Maximum items to hold in queue
     */
    public BackgroundWriter(
        FlushStrategy<T> flushStrategy,
        Duration flushInterval,
        int batchSize,
        int queueCapacity
    ) {
        this.flushStrategy = flushStrategy;
        this.flushInterval = flushInterval;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.running = new AtomicBoolean(true);
    }

    /**
     * Starts the background flush thread.
     */
    public void start() {
        workerThread = new Thread(this::processQueue, "BackgroundWriter");
        workerThread.start();
    }

    /**
     * Queues an item to be written.
     * Blocks if queue is full.
     *
     * @param item Item to queue
     * @throws InterruptedException if interrupted while waiting
     */
    public void write(T item) throws InterruptedException {
        queue.put(item);
    }

    private void processQueue() {
        List<T> batch = new ArrayList<>(batchSize);

        while (running.get() || !queue.isEmpty()) {
            try {
                T item = queue.poll(flushInterval.toMillis(), TimeUnit.MILLISECONDS);

                if (item != null) {
                    batch.add(item);
                }

                if (batch.size() >= batchSize || (item == null && !batch.isEmpty())) {
                    flushStrategy.flush(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Log error but keep running
                e.printStackTrace();
            }
        }

        // Final flush
        if (!batch.isEmpty()) {
            try {
                flushStrategy.flush(batch);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        if (workerThread != null) {
            workerThread.join(flushInterval.toMillis() * 2);
        }
    }
}