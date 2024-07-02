package cn.edu.tsinghua.iginx.parquet.util;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class ParseUtils {
  private ParseUtils() {}

  public static Optional<Integer> getOptionalInteger(Map<String, String> properties, String key) {
    return getOptional(properties, key, Integer::parseInt);
  }

  public static Optional<Long> getOptionalLong(Map<String, String> properties, String key) {
    return getOptional(properties, key, Long::parseLong);
  }

  public static Optional<Duration> getOptionalDuration(Map<String, String> properties, String key) {
    return getOptional(properties, key, Duration::parse);
  }

  public static Optional<Boolean> getOptionalBoolean(Map<String, String> properties, String key) {
    return getOptional(properties, key, Boolean::parseBoolean);
  }

  public static Optional<String> getOptionalString(Map<String, String> properties, String key) {
    return getOptional(properties, key, str -> str);
  }

  private static <V> Optional<V> getOptional(
      Map<String, String> properties, String key, Function<String, V> parser) {
    String value = properties.get(key);
    if (value == null) {
      return Optional.empty();
    }

    V valueV = parser.apply(value);
    return Optional.of(valueV);
  }

  public static void checkNonNegative(int value) {
    if (value < 0) {
      throw new IllegalArgumentException("The value must be non-negative, but got " + value);
    }
  }

  public static void checkNonNegative(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("The value must be non-negative, but got " + value);
    }
  }

  public static void checkNonNegative(Duration value) {
    if (value.isNegative()) {
      throw new IllegalArgumentException("The value must be non-negative, but got " + value);
    }
  }

  public static void checkPositive(int value) {
    if (value <= 0) {
      throw new IllegalArgumentException("The value must be positive, but got " + value);
    }
  }

  public static void checkPositive(long value) {
    if (value <= 0) {
      throw new IllegalArgumentException("The value must be positive, but got " + value);
    }
  }

  public static void checkPositive(Duration value) {
    if (value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException("The value must be positive, but got " + value);
    }
  }

  public static Consumer<String> in(String... values) {
    return value -> {
      for (String v : values) {
        if (v.equals(value)) {
          return;
        }
      }
      throw new IllegalArgumentException(
          "The value must be one of " + String.join(", ", values) + ", but got " + value);
    };
  }
}
