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
   * Starting writes...
   * Time for 1000000 write operations: 7790.20 ms
   *
   * Final sizes:
   * MemTable size: 0
   * Level 0 size: 1000
   * Level 1 size: 1000
   * Level 2 size: 1000
   * Level 3 size: 1000
   * Level 4 size: 996000
   *
   * Starting reads...
   *
   * Time for 100000 read operations: 112.41 ms
   * Read hit rate: 100.00%
   */
  public static void main(String[] args) {
    LSMTree<Integer, String> lsmTree = new LSMTree<>(1000, 5);
    int numOperations = 1_000_000;
    Map<Integer, String> verificationMap = new HashMap<>();

    System.out.println("Starting writes...");
    long startTime = System.nanoTime();
    for (int i = 0; i < numOperations; i++) {
      String value = "value" + i;
      lsmTree.put(i, value);
      verificationMap.put(i, value);
    }
    long endTime = System.nanoTime();
    System.out.printf("Time for %d write operations: %.2f ms%n",
        numOperations, (endTime - startTime) / 1e6);

    System.out.println("\nFinal sizes:");
    System.out.printf("MemTable size: %d%n", lsmTree.memTable.size());
    for (int i = 0; i < lsmTree.levels.size(); i++) {
      System.out.printf("Level %d size: %d%n", i, lsmTree.levels.get(i).size());
    }

    System.out.println("\nStarting reads...");
    int numReads = 100_000;
    int found = 0;
    int missed = 0;
    startTime = System.nanoTime();
    Random random = new Random(42);

    for (int i = 0; i < numReads; i++) {
      int key = random.nextInt(numOperations);
      String expected = verificationMap.get(key);
      String actual = lsmTree.get(key);

      if (actual != null && actual.equals(expected)) {
        found++;
      } else {
        missed++;
        if (missed <= 10) {
          System.out.printf("Miss: key=%d, expected=%s, actual=%s%n",
              key, expected, actual);
        }
      }
    }
    endTime = System.nanoTime();
    System.out.printf("\nTime for %d read operations: %.2f ms%n",
        numReads, (endTime - startTime) / 1e6);
    System.out.printf("Read hit rate: %.2f%%%n", (found * 100.0) / numReads);
  }
}