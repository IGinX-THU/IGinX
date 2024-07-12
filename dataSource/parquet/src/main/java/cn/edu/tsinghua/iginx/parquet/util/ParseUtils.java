/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
