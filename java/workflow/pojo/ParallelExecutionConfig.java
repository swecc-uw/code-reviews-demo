package workflow.pojo;

public record ParallelExecutionConfig(
    int maxThreads,
    long timeoutSeconds) {
  public static ParallelExecutionConfig defaultConfig() {
    return new ParallelExecutionConfig(
        Runtime.getRuntime().availableProcessors(),
        30);
  }
}
