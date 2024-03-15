/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
