package workflow.example;

import java.util.Random;

import workflow.WorkflowExecutor;
import workflow.WorkflowStage;
import workflow.pojo.StageResult;
import workflow.retry.ExponentialBackoffStrategy;

// example with intentional error
public class FaultyExampleFlow {
    public record ProcessingState(
            String input,
            String processedData,
            boolean isValidated) {
    }

    private static class UnreliableProcessor implements WorkflowStage<ProcessingState> {
        private final Random random = new Random();

        @Override
        public StageResult<ProcessingState> execute(ProcessingState input) {
            if (random.nextDouble() < 0.7) { // 70% chance of failure
                throw new RuntimeException("Random processing failure");
            }

            String processed = input.input().toUpperCase();
            return StageResult.continue_(new ProcessingState(
                    input.input(),
                    processed,
                    input.isValidated()));
        }
    }

    private static class Validator implements WorkflowStage<ProcessingState> {
        private final Random random = new Random();

        @Override
        public StageResult<ProcessingState> execute(ProcessingState input) {
            if (random.nextDouble() < 0.3) { // 30% chance of failure
                throw new RuntimeException("Random validation failure");
            }

            return StageResult.terminate(new ProcessingState(
                    input.input(),
                    input.processedData(),
                    true));
        }
    }

    public static void main(String[] args) {
        ProcessingState initialState = new ProcessingState("test data", "", false);

        WorkflowExecutor<ProcessingState> executor = new WorkflowExecutor.Builder<ProcessingState>()
                .withInitialStage(new UnreliableProcessor())
                .withRetryStrategy(new ExponentialBackoffStrategy(5, 100))
                .withDebug(true)
                .build();

        ProcessingState result = executor.execute(initialState);
        System.out.println("Processing completed: " + result);
    }
}
