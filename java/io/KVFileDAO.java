package io;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

record KVEntry(String key, String value) {
  private static final String DELIMITER = "†";

  public KVEntry {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(value, "value cannot be null");
    if (key.contains(DELIMITER)) {
      throw new IllegalArgumentException("Key cannot contain delimiter: " + DELIMITER);
    }
    if (value.contains(DELIMITER)) {
      throw new IllegalArgumentException("Value cannot contain delimiter: " + DELIMITER);
    }
  }

  /**
   * Parses a line into a KVEntry.
   *
   * @param line the line to parse
   * @return the parsed entry or empty if invalid
   */
  public static Optional<KVEntry> fromString(String line) {
    try {
      String[] parts = line.split(DELIMITER, 2);
      if (parts.length != 2) {
        return Optional.empty();
      }
      return Optional.of(new KVEntry(parts[0], parts[1]));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return key + DELIMITER + value;
  }
}

/**
 * A very bad key-value store implementation using a file.
 */
public class KVFileDAO implements KVDAO<String, String, KVEntry>, AutoCloseable {
  private final Path filePath;
  private final FileChannel channel;
  private volatile boolean closed = false;

  /**
   * Creates a new KVFileDAO backed by a file at the given path.
   *
   * @param path the path to the file
   * @throws IOException if the file cannot be created or accessed
   */
  public KVFileDAO(String path) throws IOException {
    this.filePath = Path.of(path);
    Files.createDirectories(this.filePath.getParent());
    Files.createFile(this.filePath);
    this.channel = FileChannel.open(filePath,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE);
  }

  /**
   * Executes an operation with a file lock.
   *
   * @param operation the operation to execute
   * @param <T>       the return type of the operation
   * @return the result of the operation
   * @throws IOException if an I/O error occurs
   */
  private <T> T withLock(IOSupplier<T> operation) throws IOException {
    checkClosed();
    try (FileLock lock = channel.lock()) {
      return operation.get();
    }
  }

  /**
   * Reads all entries from the file.
   *
   * @return list of all entries
   * @throws IOException if an I/O error occurs
   */
  private List<KVEntry> readAllEntries() throws IOException {
    return Files.readAllLines(filePath).stream()
        .map(KVEntry::fromString)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  /**
   * Writes all entries to the file.
   *
   * @param entries the entries to write
   * @throws IOException if an I/O error occurs
   */
  private void writeAllEntries(List<KVEntry> entries) throws IOException {
    Files.write(filePath,
        entries.stream()
            .map(KVEntry::toString)
            .collect(Collectors.toList()),
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  @Override
  public void batchPut(KVEntry[] items) {
    try {
      withLock(() -> {
        List<KVEntry> entries = readAllEntries();

        // Remove existing entries with same keys
        entries.removeIf(existing -> Arrays.stream(items)
            .anyMatch(item -> item.key().equals(existing.key())));

        // Add new entries
        entries.addAll(Arrays.asList(items));

        writeAllEntries(entries);
        return null;
      });
    } catch (IOException e) {
      throw new RuntimeException("Failed to batch put entries", e);
    }
  }

  @Override
  public void put(String key, String value) {
    batchPut(new KVEntry[] { new KVEntry(key, value) });
  }

  @Override
  public String[] batchGet(String[] keys) {
    try {
      return withLock(() -> {
        List<KVEntry> entries = readAllEntries();
        return Arrays.stream(keys)
            .map(key -> entries.stream()
                .filter(entry -> entry.key().equals(key))
                .findFirst()
                .map(KVEntry::value)
                .orElse(null))
            .toArray(String[]::new);
      });
    } catch (IOException e) {
      throw new RuntimeException("Failed to batch get entries", e);
    }
  }

  @Override
  public String get(String key) {
    String[] results = batchGet(new String[] { key });
    return results[0];
  }

  @Override
  public String[] batchDelete(String[] keys) {
    try {
      return withLock(() -> {
        List<KVEntry> entries = readAllEntries();

        // Find values to return
        String[] values = Arrays.stream(keys)
            .map(key -> entries.stream()
                .filter(entry -> entry.key().equals(key))
                .findFirst()
                .map(KVEntry::value)
                .orElse(null))
            .toArray(String[]::new);

        // Remove entries
        entries.removeIf(entry -> Arrays.asList(keys).contains(entry.key()));

        writeAllEntries(entries);
        return values;
      });
    } catch (IOException e) {
      throw new RuntimeException("Failed to batch delete entries", e);
    }
  }

  @Override
  public void delete(String key) {
    batchDelete(new String[] { key });
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("DAO is closed");
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      channel.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close file channel", e);
    }
  }

  // Functional interface for operations that throw IOException
  @FunctionalInterface
  private interface IOSupplier<T> {
    T get() throws IOException;
  }
}