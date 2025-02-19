package quota;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Comprehensive benchmark for the DistributedQuota system.
 * Tests various scenarios including:
 * - Single-threaded throughput
 * - Multi-threaded concurrent access
 * - Burst handling
 * - Different quota configurations
 * - System behavior under sustained load
 */
public class QuotaBenchmark {
  private static final int WARMUP_ITERATIONS = 1000;
  private static final Duration BENCHMARK_DURATION = Duration.ofSeconds(10);
  private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
  private static final boolean DEBUG = true;

  private record BenchmarkResult(
      String name,
      long totalRequests,
      long successfulRequests,
      long durationMs,
      double requestsPerSecond,
      double successRate,
      double avgLatencyMs,
      double p95LatencyMs,
      double p99LatencyMs) {
    @Override
    public String toString() {
      return String.format("""
          Benchmark: %s
          Total Requests: %d
          Successful Requests: %d
          Duration: %dms
          Throughput: %.2f req/sec
          Success Rate: %.2f%%
          Average Latency: %.2fms
          P95 Latency: %.2fms
          P99 Latency: %.2fms
          """,
          name, totalRequests, successfulRequests, durationMs,
          requestsPerSecond, successRate * 100,
          avgLatencyMs, p95LatencyMs, p99LatencyMs);
    }
  }

  private static class LatencyRecorder {
    private final List<Long> latencies = new ArrayList<>();
    private final LongAdder totalLatency = new LongAdder();
    private final AtomicInteger count = new AtomicInteger();

    void recordLatency(long latencyNanos) {
      synchronized (latencies) {
        latencies.add(latencyNanos);
      }
      totalLatency.add(latencyNanos);
      count.incrementAndGet();
    }

    double getAverageLatencyMs() {
      return (totalLatency.sum() / (double) count.get()) / 1_000_000.0;
    }

    double getPercentileLatencyMs(double percentile) {
      synchronized (latencies) {
        latencies.sort(null);
        int index = (int) (latencies.size() * percentile);
        return latencies.get(index) / 1_000_000.0;
      }
    }
  }

  /**
   * Runs a single benchmark scenario
   */
  private static BenchmarkResult runScenario(
      String name,
      QuotaManager quota,
      int numThreads,
      Duration duration,
      Consumer<QuotaManager> workload) throws Exception {

    // Setup tracking
    LongAdder totalRequests = new LongAdder();
    LongAdder successfulRequests = new LongAdder();
    LatencyRecorder latencies = new LatencyRecorder();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    if (DEBUG) {
      System.out.println("Running benchmark: " + name);
      System.out.println("Threads: " + numThreads);
      System.out.println("Duration: " + duration);
      System.out.println("Warming up...");
    }

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      workload.accept(quota);
    }

    if (DEBUG) {
      System.out.println("Benchmarking...");
    }

    // Run benchmark
    Instant start = Instant.now();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      futures.add(CompletableFuture.runAsync(() -> {
        while (Duration.between(start, Instant.now()).compareTo(duration) < 0) {
          long startNanos = System.nanoTime();
          boolean success = true;

          try {
            workload.accept(quota);
          } catch (Exception e) {
            success = false;
            e.printStackTrace();
          }

          long latencyNanos = System.nanoTime() - startNanos;
          latencies.recordLatency(latencyNanos);
          totalRequests.increment();
          if (success) {
            successfulRequests.increment();
          }
        }
      }, executor));
    }

    // Wait for completion
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    Duration elapsed = Duration.between(start, Instant.now());

    if (DEBUG) {
      System.out.println("Benchmark complete, shutting down...");
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    if (DEBUG) {
      System.out.println("Calculating results...");
    }

    // Calculate results
    long totalReqs = totalRequests.sum();
    long successfulReqs = successfulRequests.sum();
    double reqPerSec = totalReqs / (elapsed.toMillis() / 1000.0);
    double successRate = successfulReqs / (double) totalReqs;

    return new BenchmarkResult(
        name,
        totalReqs,
        successfulReqs,
        elapsed.toMillis(),
        reqPerSec,
        successRate,
        latencies.getAverageLatencyMs(),
        latencies.getPercentileLatencyMs(0.95),
        latencies.getPercentileLatencyMs(0.99));
  }

  public static void main(String[] args) throws Exception {
    if (DEBUG) {
      System.out.println("Running benchmark scenarios...");
    }
    // Create test directory
    Path testDir = Files.createTempDirectory("quota-benchmark");
    List<BenchmarkResult> results = new ArrayList<>();

    try {
      // Scenario 1: Light load, single user
      try (QuotaManager quota = new QuotaManager(
          "node1",
          testDir.resolve("light-load.db").toString())) {

        results.add(runScenario(
            "Light Load - Single User",
            quota,
            1,
            BENCHMARK_DURATION,
            q -> q.tryConsume("user1", 1, 1000, 100.0)));
      }

      // Scenario 2: Heavy load, single user
      try (QuotaManager quota = new QuotaManager(
          "node1",
          testDir.resolve("heavy-load.db").toString())) {

        results.add(runScenario(
            "Heavy Load - Single User",
            quota,
            NUM_THREADS,
            BENCHMARK_DURATION,
            q -> q.tryConsume("user1", 1, 1000, 100.0)));
      }

      // Scenario 3: Multi-user random access
      try (QuotaManager quota = new QuotaManager(
          "node1",
          testDir.resolve("multi-user.db").toString())) {

        var userQuotas = new ConcurrentHashMap<String, Double>();
        for (int i = 0; i < 100; i++) {
          userQuotas.put("user" + i, ThreadLocalRandom.current().nextDouble(10.0, 100.0));
        }

        results.add(runScenario(
            "Multi-User Random Access",
            quota,
            NUM_THREADS,
            BENCHMARK_DURATION,
            q -> {
              String randomUser = "user" + ThreadLocalRandom.current().nextInt(100);
              double rate = userQuotas.get(randomUser);
              q.tryConsume(randomUser, 1, (long) (rate * 10), rate);
            }));
      }

      // Scenario 4: Bursty traffic
      try (QuotaManager quota = new QuotaManager(
          "node1",
          testDir.resolve("bursty.db").toString())) {

        results.add(runScenario(
            "Bursty Traffic",
            quota,
            NUM_THREADS,
            BENCHMARK_DURATION,
            q -> {
              int burstSize = ThreadLocalRandom.current().nextInt(1, 20);
              q.tryConsume("user1", burstSize, 1000, 100.0);
            }));
      }

      // Scenario 5: Tight quotas
      try (QuotaManager quota = new QuotaManager(
          "node1",
          testDir.resolve("tight-quotas.db").toString())) {

        results.add(runScenario(
            "Tight Quotas",
            quota,
            NUM_THREADS,
            BENCHMARK_DURATION,
            q -> q.tryConsume("user1", 1, 10, 1.0)));
      }

      // Print results
      System.out.println("=== Benchmark Results ===\n");
      results.forEach(result -> {
        System.out.println(result);
        System.out.println("-------------------\n");
      });

    } finally {
      // Cleanup
      Files.walk(testDir)
          .sorted((a, b) -> b.compareTo(a))
          .forEach(path -> {
            try {
              Files.delete(path);
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }
  }
}