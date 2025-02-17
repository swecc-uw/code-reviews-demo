package io;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A quota management system that tracks usage.
 */
public class QuotaManager implements AutoCloseable {
    public static final int DEFAULT_CAPACITY = 1000;
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_SYNC_INTERVAL_SECONDS = 5;
    private final String nodeId;
    private final KVFileDAO store;
    private final BackgroundWriter<QuotaUpdate> writer;
    private final Map<String, TokenBucket> localBuckets;
    private final Map<String, AtomicLong> localCounters;

    /**
     * A quota update that needs to be persisted
     */
    private record QuotaUpdate(
            String userId,
            long tokensUsed,
            Instant timestamp) {
    }

    /**
     * Token bucket algorithm for rate limiting
     */
    private static class TokenBucket {
        private final long capacity;
        private final double refillRate;
        private double tokens;
        private long lastRefill;

        TokenBucket(long capacity, double tokensPerSecond) {
            this.capacity = capacity;
            this.refillRate = tokensPerSecond;
            this.tokens = capacity;
            this.lastRefill = System.currentTimeMillis();
        }

        synchronized boolean tryConsume(long requested) {
            refill();
            if (tokens >= requested) {
                tokens -= requested;
                return true;
            }
            return false;
        }

        private void refill() {
            long now = System.currentTimeMillis();
            double elapsed = (now - lastRefill) / 1000.0;
            tokens = Math.min(capacity, tokens + (refillRate * elapsed));
            lastRefill = now;
        }
    }

    /**
     * @param nodeId       Unique identifier for this node
     * @param storePath    Path to persistent storage
     * @param syncInterval How often to sync with storage
     * @throws IOException if storage cannot be initialized
     */
    public QuotaManager(String nodeId, String storePath, Duration syncInterval, int batchSize, int queueCapacity)
            throws IOException {
        this.nodeId = nodeId;
        this.store = new KVFileDAO(storePath);
        this.localBuckets = new ConcurrentHashMap<>();
        this.localCounters = new ConcurrentHashMap<>();

        // set up background writer for quota updates
        this.writer = new BackgroundWriter<>(
                this::flushQuotaUpdates,
                syncInterval,
                batchSize,
                queueCapacity);

        writer.start();
    }

    public QuotaManager(String nodeId, String storePath) throws IOException {
        this(
                nodeId,
                storePath,
                Duration.ofSeconds(DEFAULT_SYNC_INTERVAL_SECONDS),
                DEFAULT_BATCH_SIZE,
                DEFAULT_CAPACITY);
    }

    /**
     * Tries to consume quota for a user. Thread-safe and eventually consistent.
     *
     * @param userId          User or resource identifier
     * @param tokens          Number of tokens to consume
     * @param capacity        Maximum tokens allowed
     * @param tokensPerSecond Rate of token replenishment
     * @return true if quota was consumed, false if exceeded
     */
    public boolean tryConsume(String userId, long tokens, long capacity, double tokensPerSecond) {
        TokenBucket bucket = localBuckets.computeIfAbsent(userId,
                k -> new TokenBucket(capacity, tokensPerSecond));

        if (bucket.tryConsume(tokens)) {
            AtomicLong counter = localCounters.computeIfAbsent(userId, k -> new AtomicLong());
            long used = counter.addAndGet(tokens);

            try {
                writer.write(new QuotaUpdate(userId, tokens, Instant.now()));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    /**
     * Gets the current usage for a user across all nodes
     *
     * @param userId User or resource identifier
     * @return total tokens used
     */
    public long getCurrentUsage(String userId) {
        String storedValue = store.get(userId);
        if (storedValue == null) {
            return 0;
        }

        // add local pending updates
        AtomicLong local = localCounters.get(userId);
        long localCount = local != null ? local.get() : 0;

        return Long.parseLong(storedValue) + localCount;
    }

    /**
     * Background flush of quota updates to persistent storage
     */
    private void flushQuotaUpdates(List<QuotaUpdate> updates) throws Exception {
        // group by user and sum
        Map<String, Long> aggregated = new HashMap<>();
        for (QuotaUpdate update : updates) {
            aggregated.merge(update.userId(), update.tokensUsed(), Long::sum);
        }

        // update persistent storage
        List<KVFileEntry> entries = new ArrayList<>();
        for (var entry : aggregated.entrySet()) {
            String userId = entry.getKey();
            long delta = entry.getValue();

            String current = store.get(userId);
            long newValue = (current != null ? Long.parseLong(current) : 0) + delta;

            entries.add(new KVFileEntry(userId, String.valueOf(newValue)));
        }

        store.batchPut(entries.toArray(new KVFileEntry[0]));

        // update local counters
        for (var entry : aggregated.entrySet()) {
            AtomicLong counter = localCounters.get(entry.getKey());
            if (counter != null) {
                counter.addAndGet(-entry.getValue());
            }
        }
    }

    @Override
    public void close() throws Exception {
        writer.close();
        store.close();
    }
}