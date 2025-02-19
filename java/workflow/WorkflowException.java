package workflow;

public class WorkflowException extends RuntimeException {
  public WorkflowException(String message, Throwable cause) {
    super(message, cause);
  }
}