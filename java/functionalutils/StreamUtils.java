package functionalutils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.*;

/**
 * Utility class providing helpful stream operations for common use cases.
 * All methods are thread-safe unless otherwise specified.
 */
public final class StreamUtils {
    private StreamUtils() {
    } // Prevent instantiation

    /**
     * Creates a stream of pairs from two lists of equal size. Each pair contains
     * corresponding elements from both lists at the same index.
     *
     * @param <T>    Type of elements in first list
     * @param <U>    Type of elements in second list
     * @param first  The first list
     * @param second The second list
     * @return A stream of pairs containing elements from both lists
     * @throws IllegalArgumentException if lists are of different sizes
     *
     *                                  Runtime: O(n) where n is the size of the
     *                                  lists
     *                                  Space: O(1) for the stream itself (excluding
     *                                  final collection)
     *
     *                                  Example:
     *
     *                                  <pre>{@code
     * List<String> names = Arrays.asList("Alice", "Bob");
     * List<Integer> ages = Arrays.asList(25, 30);
     * zip(names, ages).forEach(pair ->
     *     System.out.println(pair.getKey() + ": " + pair.getValue()));
     * }</pre>
     */
    public static <T, U> Stream<Map.Entry<T, U>> zip(List<T> first, List<U> second) {
        if (first.size() != second.size()) {
            throw new IllegalArgumentException("Lists must be of equal size");
        }
        return IntStream.range(0, first.size())
                .mapToObj(i -> Map.entry(first.get(i), second.get(i)));
    }

    /**
     * Returns a predicate that filters distinct elements based on a key extractor.
     * Useful for filtering unique elements by a specific field while maintaining
     * the first occurrence of each distinct value.
     *
     * @param <T>          The type of elements to filter
     * @param keyExtractor Function to extract the key to determine uniqueness
     * @return A predicate that returns true for the first occurrence of each key
     *
     *         Runtime: O(1) per element
     *         Space: O(n) where n is the number of unique keys
     *         Thread-safe: Uses ConcurrentHashMap internally
     *
     *         Example:
     *
     *         <pre>{@code
     * Stream<Person> persons = Stream.of(new Person("NY"), new Person("NY"));
     * persons.filter(distinctBy(Person::getState))
     *        .forEach(System.out::println); // Prints only first NY person
     * }</pre>
     */
    public static <T> Predicate<T> distinctBy(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    /**
     * Chunks a stream into fixed-size lists. The last chunk may be smaller than
     * the requested size if the stream's size is not evenly divisible by the chunk
     * size.
     *
     * @param <T>    The type of stream elements
     * @param stream The input stream
     * @param size   The size of each chunk
     * @return A stream of lists, each containing up to 'size' elements
     * @throws IllegalArgumentException if size is less than 1
     *
     *                                  Runtime: O(n) where n is the stream size
     *                                  Space: O(size) for each chunk
     *
     *                                  Example:
     *
     *                                  <pre>{@code
     * Stream<Integer> numbers = Stream.of(1, 2, 3, 4, 5);
     * chunk(numbers, 2).forEach(System.out::println); // [1,2], [3,4], [5]
     * }</pre>
     */
    public static <T> Stream<List<T>> chunk(Stream<T> stream, int size) {
        if (size < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        Iterator<T> iterator = stream.iterator();
        return Stream.generate(() -> {
            List<T> chunk = new ArrayList<>(size);
            for (int i = 0; i < size && iterator.hasNext(); i++) {
                chunk.add(iterator.next());
            }
            return chunk;
        }).takeWhile(chunk -> !chunk.isEmpty());
    }

    /**
     * Returns a collector that collects elements into an unmodifiable list.
     * The order of elements is preserved.
     *
     * @param <T> The type of elements to collect
     * @return A collector producing an immutable list
     *
     *         Runtime: O(n) where n is the number of elements
     *         Space: O(n)
     *
     *         Example:
     *
     *         <pre>{@code
     * List<String> immutable = stream.collect(toImmutableList());
     * immutable.add("new"); // Throws UnsupportedOperationException
     * }</pre>
     */
    public static <T> Collector<T, ?, List<T>> toImmutableList() {
        return Collectors.collectingAndThen(
                Collectors.toList(),
                Collections::unmodifiableList);
    }

    /**
     * Returns a collector that collects elements into an unmodifiable set.
     * The iteration order is not guaranteed.
     *
     * @param <T> The type of elements to collect
     * @return A collector producing an immutable set
     *
     *         Runtime: O(n) where n is the number of elements
     *         Space: O(n)
     *
     *         Example:
     *
     *         <pre>{@code
     * Set<String> immutable = stream.collect(toImmutableSet());
     * }</pre>
     */
    public static <T> Collector<T, ?, Set<T>> toImmutableSet() {
        return Collectors.collectingAndThen(
                Collectors.toSet(),
                Collections::unmodifiableSet);
    }

    /**
     * Creates a map from a stream where values might be null.
     * In case of duplicate keys, later values overwrite earlier ones.
     *
     * @param <T>         The type of elements in the stream
     * @param <K>         The type of keys in the map
     * @param <U>         The type of values in the map
     * @param keyMapper   Function to extract the key
     * @param valueMapper Function to extract the value
     * @return A collector producing a map that allows null values
     *
     *         Runtime: O(n) where n is the number of elements
     *         Space: O(n)
     *
     *         Example:
     *
     *         <pre>{@code
     * Map<String, String> map = stream.collect(toNullableMap(
     *     Person::getName,
     *     Person::getNullableEmail
     * ));
     * }</pre>
     */
    public static <T, K, U> Collector<T, ?, Map<K, U>> toNullableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends U> valueMapper) {
        return Collectors.toMap(
                keyMapper,
                valueMapper,
                (v1, v2) -> v2,
                HashMap::new);
    }

    /**
     * Returns elements that appear exactly n times in the stream.
     * Note: This operation requires storing the entire stream in memory.
     *
     * @param <T>    The type of elements to filter
     * @param stream The input stream
     * @param n      The exact number of occurrences to match
     * @return A predicate that returns true for elements occurring exactly n times
     *
     *         Runtime: O(n) for frequency calculation, O(1) per predicate test
     *         Space: O(d) where d is the number of distinct elements
     *
     *         Example:
     *
     *         <pre>{@code
     * Stream<String> words = Stream.of("a", "a", "b", "c", "c", "c");
     * words.filter(occursExactly(words, 2))
     *      .forEach(System.out::println); // Prints "a"
     * }</pre>
     */
    public static <T> Predicate<T> occursExactly(Stream<T> stream, long n) {
        Map<T, Long> frequency = stream.collect(
                Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return item -> frequency.get(item) == n;
    }

    /**
     * Retries a stream operation until it succeeds or maximum attempts are reached.
     * Introduces a delay between retries.
     *
     * @param <T>         The type of input elements
     * @param <R>         The type of output elements
     * @param stream      The input stream
     * @param mapper      The operation to retry
     * @param maxAttempts Maximum number of attempts per element
     * @param delayMs     Delay between retries in milliseconds
     * @return A stream of successfully processed elements
     * @throws RuntimeException if max attempts are reached for any element
     *
     *                          Runtime: O(m*n) where m is maxAttempts and n is
     *                          stream size
     *                          Space: O(1) excluding the resulting stream
     *
     *                          Example:
     *
     *                          <pre>{@code
     * Stream<String> urls = Stream.of("url1", "url2");
     * retry(urls, this::fetchUrl, 3, 1000)
     *     .forEach(System.out::println);
     * }</pre>
     */
    public static <T, R> Stream<R> retry(
            Stream<T> stream,
            Function<? super T, ? extends R> mapper,
            int maxAttempts,
            long delayMs) {
        return stream.map(item -> {
            Exception lastException = null;
            for (int attempt = 0; attempt < maxAttempts; attempt++) {
                try {
                    return mapper.apply(item);
                } catch (Exception e) {
                    lastException = e;
                    if (attempt < maxAttempts - 1) {
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(ie);
                        }
                    }
                }
            }
            throw new RuntimeException("Failed after " + maxAttempts + " attempts", lastException);
        });
    }

    /**
     * Creates a stream of all combinations of elements from multiple lists.
     * The resulting lists contain one element from each input list.
     *
     * @param <T>   The type of elements
     * @param lists The input lists
     * @return A stream of all possible combinations
     *
     *         Runtime: O(n^m) where n is average list size and m is number of lists
     *         Space: O(m) per combination
     *
     *         Example:
     *
     *         <pre>{@code
     * List<String> colors = Arrays.asList("red", "blue");
     * List<String> sizes = Arrays.asList("S", "M");
     * cartesianProduct(colors, sizes)
     *     .forEach(System.out::println); // [red,S], [red,M], [blue,S], [blue,M]
     * }</pre>
     */
    @SafeVarargs
    public static <T> Stream<List<T>> cartesianProduct(List<T>... lists) {
        if (lists.length == 0) {
            return Stream.empty();
        }
        return Arrays.stream(lists)
                .reduce(
                        Stream.of(new ArrayList<T>()),
                        (acc, list) -> acc.flatMap(l -> list.stream()
                                .map(element -> {
                                    List<T> newList = new ArrayList<>(l);
                                    newList.add(element);
                                    return newList;
                                })),
                        Stream::concat);
    }
}