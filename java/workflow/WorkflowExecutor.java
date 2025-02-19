package workflow;

import java.util.concurrent.Executors;
import java.util.logging.Logger;

import workflow.pojo.ParallelExecutionConfig;
import workflow.pojo.StageResult;
import workflow.retry.RetryStrategy;
import java.util.concurrent.ExecutorService;

public class WorkflowExecutor<S> {
  private static final Logger LOGGER = Logger.getLogger(WorkflowExecutor.class.getName());

  private final WorkflowStage<S> initialStage;
  private final RetryStrategy retryStrategy;
  private final ExecutorService executor;
  private final ParallelExecutionConfig parallelConfig;
  private final boolean debug;

  private WorkflowExecutor(
      WorkflowStage<S> initialStage,
      RetryStrategy retryStrategy,
      ParallelExecutionConfig parallelConfig,
      boolean debug) {
    this.initialStage = initialStage;
    this.retryStrategy = retryStrategy;
    this.parallelConfig = parallelConfig;
    this.debug = debug;
    this.executor = Executors.newVirtualThreadPerTaskExecutor();

    if (debug) {
      LOGGER.info("WorkflowExecutor initialized with initial stage: " + initialStage.getName());
    }
  }

  public S execute(S initialState) throws WorkflowException {
    try {
      return executeStage(initialState, initialStage);
    } finally {
      executor.shutdown();
    }
  }

  private S executeStage(S state, WorkflowStage<S> stage) throws WorkflowException {
    int attemptCount = 0;
    Exception lastException = null;

    while (true) {
      try {
        StageResult<S> result = stage.execute(state);
        if (!result.shouldContinue()) {
          return result.state();
        }

        return result.nextStage()
            .map(next -> executeStage(result.state(), next))
            .orElse(result.state());

      } catch (Exception e) {
        lastException = e;
        attemptCount++;

        if (!retryStrategy.shouldRetry(attemptCount, e)) {
          throw new WorkflowException(
              "Stage " + stage.getName() + " failed after " + attemptCount + " attempts",
              e);
        }

        try {
          Thread.sleep(retryStrategy.getDelayMillis(attemptCount));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new WorkflowException("Workflow interrupted during retry delay", ie);
        }
      }
    }
  }

  public static class Builder<S> {
    private WorkflowStage<S> initialStage;
    private RetryStrategy retryStrategy = RetryStrategy.defaultStrategy();
    private ParallelExecutionConfig parallelConfig = ParallelExecutionConfig.defaultConfig();
    private boolean debug = false;

    public Builder<S> withInitialStage(WorkflowStage<S> stage) {
      this.initialStage = stage;
      return this;
    }

    public Builder<S> withRetryStrategy(RetryStrategy strategy) {
      this.retryStrategy = strategy;
      return this;
    }

    public Builder<S> withParallelConfig(ParallelExecutionConfig config) {
      this.parallelConfig = config;
      return this;
    }

    public Builder<S> withDebug(boolean debug) {
      this.debug = debug;
      return this;
    }

    public WorkflowExecutor<S> build() {
      if (initialStage == null) {
        throw new IllegalStateException("Initial stage must be set");
      }
      return new WorkflowExecutor<>(initialStage, retryStrategy, parallelConfig, debug);
    }
  }
}