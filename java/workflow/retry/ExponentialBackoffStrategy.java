package workflow.retry;

public record ExponentialBackoffStrategy(
        int maxAttempts,
        long baseDelayMillis) implements RetryStrategy {
    @Override
    public boolean shouldRetry(int attemptCount, Exception e) {
        return attemptCount < maxAttempts;
    }

    @Override
    public long getDelayMillis(int attemptCount) {
        return baseDelayMillis * (1L << (attemptCount - 1));
    }
}
