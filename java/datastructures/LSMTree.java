package datastructures;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LSMTree<K extends Comparable<K>, V> {
  private final int maxMemTableSize;
  private final int maxLevels;
  private final List<SortedMap<K, V>> levels;
  private ConcurrentSkipListMap<K, V> memTable;
  private final AtomicInteger memTableSize;

  public LSMTree(int maxMemTableSize, int maxLevels) {
    this.maxMemTableSize = maxMemTableSize;
    this.maxLevels = maxLevels;
    this.levels = new ArrayList<>(maxLevels);
    for (int i = 0; i < maxLevels; i++) {
      levels.add(new TreeMap<>());
    }
    this.memTable = new ConcurrentSkipListMap<>();
    this.memTableSize = new AtomicInteger(0);
  }

  public void put(K key, V value) {
    memTable.put(key, value);
    if (memTableSize.incrementAndGet() >= maxMemTableSize) {
      flushMemTable();
    }
  }

  public V get(K key) {
    V value = memTable.get(key);
    if (value != null)
      return value;

    for (SortedMap<K, V> level : levels) {
      value = level.get(key);
      if (value != null)
        return value;
    }
    return null;
  }

  private synchronized void flushMemTable() {
    SortedMap<K, V> newLevel = new TreeMap<>(memTable);
    levels.add(0, newLevel);
    memTable = new ConcurrentSkipListMap<>();
    memTableSize.set(0);

    if (levels.size() > maxLevels) {
      compactLevels();
    }
  }

  private void compactLevels() {
    SortedMap<K, V> lastLevel = levels.remove(levels.size() - 1);
    SortedMap<K, V> secondLastLevel = levels.remove(levels.size() - 1);

    SortedMap<K, V> mergedLevel = new TreeMap<>(lastLevel);
    mergedLevel.putAll(secondLastLevel);

    levels.add(mergedLevel);
  }

  /*
   * stress test, last run on MacBook Pro 2022
   * Time for 1000000 write operations: 6243.41 ms
   * Time for 100000 read operations: 130.64 ms
   * Read hit rate: 63.16%
   */
  public static void main(String[] args) {
    LSMTree<Integer, String> lsmTree = new LSMTree<>(1000, 5);
    int numOperations = 1_000_000;
    Random random = new Random();

    // preallocate value strings
    String[] values = new String[numOperations];
    for (int i = 0; i < numOperations; i++) {
      values[i] = "value" + i;
    }

    // write operations
    long startTime = System.nanoTime();
    for (int i = 0; i < numOperations; i++) {
      int key = random.nextInt(numOperations);
      String value = values[key];
      lsmTree.put(key, value);
    }
    long endTime = System.nanoTime();
    System.out.printf("Time for %d write operations: %.2f ms%n", numOperations, (endTime - startTime) / 1e6);

    // read operations
    int numReads = 100_000;
    int found = 0;
    startTime = System.nanoTime();
    for (int i = 0; i < numReads; i++) {
      int key = random.nextInt(numOperations);
      if (lsmTree.get(key) != null) {
        found++;
      }
    }
    endTime = System.nanoTime();
    System.out.printf("Time for %d read operations: %.2f ms%n", numReads, (endTime - startTime) / 1e6);
    System.out.printf("Read hit rate: %.2f%%%n", (found * 100.0) / numReads);
  }
}