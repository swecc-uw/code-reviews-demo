package workflow.pojo;

import java.util.Optional;

import workflow.WorkflowStage;

public record StageResult<S>(
        S state,
        boolean shouldContinue,
        Optional<WorkflowStage<S>> nextStage) {
    public static <S> StageResult<S> continue_(S state) {
        return new StageResult<>(state, true, Optional.empty());
    }

    public static <S> StageResult<S> continueWith(S state, WorkflowStage<S> next) {
        return new StageResult<>(state, true, Optional.of(next));
    }

    public static <S> StageResult<S> terminate(S state) {
        return new StageResult<>(state, false, Optional.empty());
    }
}
