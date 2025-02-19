package workflow.retry;

public interface RetryStrategy {
    boolean shouldRetry(int attemptCount, Exception e);

    long getDelayMillis(int attemptCount);

    static RetryStrategy defaultStrategy() {
        return new ExponentialBackoffStrategy(3, 1000);
    }
}
