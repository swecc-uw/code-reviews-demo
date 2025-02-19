package workflow.example;

import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import workflow.WorkflowExecutor;
import workflow.WorkflowStage;
import workflow.pojo.StageResult;

import java.util.Arrays;

public class MatrixBenchmark {
    public static class Matrix {
        private final double[][] data;
        private final int rows;
        private final int cols;

        public Matrix(double[][] data) {
            this.rows = data.length;
            this.cols = data[0].length;
            this.data = new double[rows][cols];
            for (int i = 0; i < rows; i++) {
                System.arraycopy(data[i], 0, this.data[i], 0, cols);
            }
        }

        public Matrix multiply(Matrix other) {
            if (this.cols != other.rows) {
                throw new IllegalArgumentException("Invalid matrix dimensions for multiplication");
            }

            double[][] result = new double[this.rows][other.cols];
            for (int i = 0; i < this.rows; i++) {
                for (int j = 0; j < other.cols; j++) {
                    double sum = 0;
                    for (int k = 0; k < this.cols; k++) {
                        sum += this.data[i][k] * other.data[k][j];
                    }
                    result[i][j] = sum;
                }
            }
            return new Matrix(result);
        }

        public Matrix inverse() {
            if (rows != cols) {
                throw new IllegalArgumentException("Matrix must be square");
            }

            int n = rows;
            double[][] augmented = new double[n][2 * n];

            // augmented matrix [A|I]
            for (int i = 0; i < n; i++) {
                System.arraycopy(data[i], 0, augmented[i], 0, n);
                augmented[i][i + n] = 1;
            }

            // gaussian elimination
            for (int i = 0; i < n; i++) {
                double pivot = augmented[i][i];
                if (Math.abs(pivot) < 1e-10) {
                    throw new IllegalStateException("Matrix is singular");
                }

                // normalize row i
                for (int j = 0; j < 2 * n; j++) {
                    augmented[i][j] /= pivot;
                }

                // elim column i from all other rows
                for (int k = 0; k < n; k++) {
                    if (k != i) {
                        double factor = augmented[k][i];
                        for (int j = 0; j < 2 * n; j++) {
                            augmented[k][j] -= factor * augmented[i][j];
                        }
                    }
                }
            }

            // inverse matrix [I|A^-1]
            double[][] inverse = new double[n][n];
            for (int i = 0; i < n; i++) {
                System.arraycopy(augmented[i], n, inverse[i], 0, n);
            }

            return new Matrix(inverse);
        }
    }

    public record MatrixState(
            List<Matrix> inputMatrices,
            List<Matrix> multipliedMatrices,
            List<Matrix> invertedMatrices,
            boolean isComplete) {
        public static MatrixState initial(List<Matrix> inputs) {
            return new MatrixState(inputs, new ArrayList<>(), new ArrayList<>(), false);
        }
    }

    private static class MatrixMultiplier implements WorkflowStage<MatrixState> {
        private final int startIdx;
        private final int endIdx;

        public MatrixMultiplier(int startIdx, int endIdx) {
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public StageResult<MatrixState> execute(MatrixState state) {
            List<Matrix> results = new ArrayList<>();
            for (int i = startIdx; i < Math.min(endIdx, state.inputMatrices().size() - 1); i++) {
                results.add(state.inputMatrices().get(i).multiply(state.inputMatrices().get(i + 1)));
            }

            List<Matrix> newMultiplied = new ArrayList<>(state.multipliedMatrices());
            newMultiplied.addAll(results);

            return StageResult.continue_(new MatrixState(
                    state.inputMatrices(),
                    newMultiplied,
                    state.invertedMatrices(),
                    false));
        }
    }

    private static class MatrixInverter implements WorkflowStage<MatrixState> {
        private final int startIdx;
        private final int endIdx;

        public MatrixInverter(int startIdx, int endIdx) {
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public StageResult<MatrixState> execute(MatrixState state) {
            List<Matrix> results = new ArrayList<>();
            for (int i = startIdx; i < Math.min(endIdx, state.multipliedMatrices().size()); i++) {
                Matrix matrix = state.multipliedMatrices().get(i);
                results.add(matrix.inverse());
            }

            List<Matrix> newInverted = new ArrayList<>(state.invertedMatrices());
            newInverted.addAll(results);

            return StageResult.continue_(new MatrixState(
                    state.inputMatrices(),
                    state.multipliedMatrices(),
                    newInverted,
                    false));
        }
    }

    private static class ResultCombiner implements WorkflowStage<MatrixState> {
        @Override
        public StageResult<MatrixState> execute(MatrixState state) {
            return StageResult.terminate(new MatrixState(
                    state.inputMatrices(),
                    state.multipliedMatrices(),
                    state.invertedMatrices(),
                    true));
        }
    }

    // for testing
    private static List<Matrix> generateRandomMatrices(int count, int size) {
        Random random = new Random();
        List<Matrix> matrices = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            double[][] data = new double[size][size];
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    data[j][k] = random.nextDouble();
                }
            }
            matrices.add(new Matrix(data));
        }

        return matrices;
    }

    private static WorkflowStage<MatrixState> createParallelWorkflow(int numMatrices) {
        return state -> {
            int chunkSize = Math.max(1, numMatrices / Runtime.getRuntime().availableProcessors());

            List<CompletableFuture<MatrixState>> multiplierFutures = new ArrayList<>();
            for (int i = 0; i < numMatrices; i += chunkSize) {
                int startIdx = i;
                int endIdx = Math.min(i + chunkSize, numMatrices);

                multiplierFutures.add(CompletableFuture
                        .supplyAsync(() -> new MatrixMultiplier(startIdx, endIdx).execute(state).state()));
            }

            MatrixState multipliedState = multiplierFutures.stream()
                    .map(CompletableFuture::join)
                    .reduce(state, (s1, s2) -> new MatrixState(
                            s1.inputMatrices(),
                            Stream.concat(s1.multipliedMatrices().stream(), s2.multipliedMatrices().stream())
                                    .collect(Collectors.toList()),
                            s1.invertedMatrices(),
                            false));

            List<CompletableFuture<MatrixState>> inverterFutures = new ArrayList<>();
            int multipliedSize = multipliedState.multipliedMatrices().size();
            for (int i = 0; i < multipliedSize; i += chunkSize) {
                int startIdx = i;
                int endIdx = Math.min(i + chunkSize, multipliedSize);

                inverterFutures.add(CompletableFuture
                        .supplyAsync(() -> new MatrixInverter(startIdx, endIdx).execute(multipliedState).state()));
            }

            MatrixState finalState = inverterFutures.stream()
                    .map(CompletableFuture::join)
                    .reduce(multipliedState, (s1, s2) -> new MatrixState(
                            s1.inputMatrices(),
                            s1.multipliedMatrices(),
                            Stream.concat(s1.invertedMatrices().stream(), s2.invertedMatrices().stream())
                                    .collect(Collectors.toList()),
                            false));

            return StageResult.continueWith(finalState, new ResultCombiner());
        };
    }

    private static WorkflowStage<MatrixState> createSequentialWorkflow(int numMatrices) {
        return new WorkflowStage<MatrixState>() {
            @Override
            public StageResult<MatrixState> execute(MatrixState state) {
                MatrixMultiplier multiplier = new MatrixMultiplier(0, numMatrices);
                MatrixState multipliedState = multiplier.execute(state).state();
                MatrixInverter inverter = new MatrixInverter(0, multipliedState.multipliedMatrices().size());
                MatrixState finalState = inverter.execute(multipliedState).state();

                return StageResult.continueWith(finalState, new ResultCombiner());
            }
        };
    }

    public static void main(String[] args) {
        // bench params
        int[] matrixSizes = { 50, 100, 200 };
        int numMatrices = 10;
        int numWarmups = 2;
        int numRuns = 5;

        for (int size : matrixSizes) {
            System.out.println("\nBenchmarking with " + size + "x" + size + " matrices:");

            // warmup
            for (int i = 0; i < numWarmups; i++) {
                List<Matrix> matrices = generateRandomMatrices(numMatrices, size);
                runWorkflow(createSequentialWorkflow(numMatrices), matrices, "Sequential Warmup");
                runWorkflow(createParallelWorkflow(numMatrices), matrices, "Parallel Warmup");
            }

            // benchmark
            long[] sequentialTimes = new long[numRuns];
            long[] parallelTimes = new long[numRuns];

            for (int i = 0; i < numRuns; i++) {
                List<Matrix> matrices = generateRandomMatrices(numMatrices, size);

                sequentialTimes[i] = runWorkflow(createSequentialWorkflow(numMatrices), matrices, "Sequential");
                parallelTimes[i] = runWorkflow(createParallelWorkflow(numMatrices), matrices, "Parallel");
            }

            double avgSequential = Arrays.stream(sequentialTimes).average().orElse(0);
            double avgParallel = Arrays.stream(parallelTimes).average().orElse(0);

            System.out.printf("Average Sequential Time: %.2f ms%n", avgSequential);
            System.out.printf("Average Parallel Time: %.2f ms%n", avgParallel);
            System.out.printf("Speedup: %.2fx%n", avgSequential / avgParallel);
        }
    }

    private static long runWorkflow(WorkflowStage<MatrixState> workflow, List<Matrix> matrices, String label) {
        MatrixState initialState = MatrixState.initial(matrices);

        WorkflowExecutor<MatrixState> executor = new WorkflowExecutor.Builder<MatrixState>()
                .withInitialStage(workflow)
                .withDebug(false)
                .build();

        long startTime = System.nanoTime();
        executor.execute(initialState);
        long endTime = System.nanoTime();

        return (endTime - startTime) / 1_000_000;
    }
}