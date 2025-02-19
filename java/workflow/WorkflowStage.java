package workflow;

import workflow.pojo.StageResult;

@FunctionalInterface
public interface WorkflowStage<S> {
    StageResult<S> execute(S input);

    default String getName() {
        return this.getClass().getSimpleName();
    }
}