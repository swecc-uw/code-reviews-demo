package io;

import java.util.Objects;
import java.util.Optional;

public record KVFileEntry(String key, String value) {
  private static final String DELIMITER = "†";

  public KVFileEntry {
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
  public static Optional<KVFileEntry> fromString(String line) {
    try {
      String[] parts = line.split(DELIMITER, 2);
      if (parts.length != 2) {
        return Optional.empty();
      }
      return Optional.of(new KVFileEntry(parts[0], parts[1]));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return key + DELIMITER + value;
  }
}