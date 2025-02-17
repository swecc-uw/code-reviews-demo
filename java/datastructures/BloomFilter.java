package datastructures;
import java.util.*;
import java.util.function.Function;

public class BloomFilter<T> {
    private BitSet bitSet;
    private int size;
    private int numHashFunctions;
    private Function<T, Integer>[] hashFunctions;

    @SuppressWarnings("unchecked")
    public BloomFilter(int size, int numHashFunctions) {
        this.size = size;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = new BitSet(size);
        this.hashFunctions = new Function[numHashFunctions];

        for (int i = 0; i < numHashFunctions; i++) {
            final int seed = i;
            hashFunctions[i] = item -> Math.abs((item.hashCode() + seed) % size);
        }
    }

    public void add(T item) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            bitSet.set(hashFunction.apply(item));
        }
    }

    public boolean mightContain(T item) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            if (!bitSet.get(hashFunction.apply(item))) {
                return false;
            }
        }
        return true;
    }

    public void clear() {
        bitSet.clear();
    }

    public int size() {
        return size;
    }

    public static void main(String[] args) {
        int size = 1_000_000;
        int numHashFunctions = 5;
        BloomFilter<String> filter = new BloomFilter<>(size, numHashFunctions);

        // Stress test parameters
        int numElementsToAdd = 100_000;
        int numTestElements = 1_000_000;

        // Generate test data
        Set<String> addedElements = new HashSet<>();
        System.out.println("Adding elements to the Bloom filter...");
        long startTime = System.nanoTime();

        for (int i = 0; i < numElementsToAdd; i++) {
            String element = UUID.randomUUID().toString();
            filter.add(element);
            addedElements.add(element);
        }

        long endTime = System.nanoTime();
        System.out.printf("Time taken to add %d elements: %.2f ms%n",
                numElementsToAdd, (endTime - startTime) / 1e6);

        // Test for false positives
        int falsePositives = 0;
        startTime = System.nanoTime();

        for (int i = 0; i < numTestElements; i++) {
            String testElement = UUID.randomUUID().toString();
            if (filter.mightContain(testElement) && !addedElements.contains(testElement)) {
                falsePositives++;
            }
        }

        endTime = System.nanoTime();

        // Calculate statistics
        double falsePositiveRate = (double) falsePositives / numTestElements;
        double queryTimePerElement = (endTime - startTime) / (double) numTestElements;

        System.out.printf("False positive rate: %.4f%n", falsePositiveRate);
        System.out.printf("Average query time: %.2f ns%n", queryTimePerElement);

        // Test for false negatives (should always be 0)
        int falseNegatives = 0;
        for (String element : addedElements) {
            if (!filter.mightContain(element)) {
                falseNegatives++;
            }
        }

        System.out.printf("False negatives: %d (should be 0)%n", falseNegatives);
    }
}