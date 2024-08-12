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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.common;

import com.google.common.collect.Range;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public abstract class AbstractConfig {

  public abstract List<ValidationProblem> validate();

  public static <C extends AbstractConfig> C of(Config raw, Class<C> clazz) {
    return ConfigBeanFactory.create(raw, clazz);
  }

  public static class ValidationProblem {
    private final List<String> reversedPath;
    private final String problem;

    public ValidationProblem(@Nullable String field, String problem) {
      this.reversedPath = new ArrayList<>();
      if (field != null) {
        this.reversedPath.add(field);
      }
      this.problem = Objects.requireNonNull(problem);
    }

    public void addParent(String parent) {
      reversedPath.add(parent);
    }

    @Override
    public String toString() {
      List<String> path = new ArrayList<>(this.reversedPath);
      Collections.reverse(path);
      return String.format("%s:'%s'", String.join(".", path), problem);
    }
  }

  public static class MissingFieldValidationProblem extends ValidationProblem {
    public MissingFieldValidationProblem(String field) {
      super(field, "missing field");
    }
  }

  public static class InvalidFieldValidationProblem extends ValidationProblem {
    public InvalidFieldValidationProblem(String field, String problem) {
      super(field, problem);
    }
  }

  public static class ValidationWarning extends ValidationProblem {
    public ValidationWarning(String field, String problem) {
      super(field, problem);
    }
  }

  protected static boolean validateNotNull(
      List<ValidationProblem> dst, String field, Object value) {
    if (value == null) {
      dst.add(new MissingFieldValidationProblem(field));
      return false;
    }
    return true;
  }

  protected static void validateSubConfig(
      List<ValidationProblem> dst, String field, AbstractConfig value) {
    if (!validateNotNull(dst, field, value)) {
      return;
    }
    assert value != null;
    dst.addAll(
        value.validate().stream().peek(p -> p.addParent(field)).collect(Collectors.toList()));
  }

  protected static void validateNotBlanks(
      List<ValidationProblem> dst, String field, @Nullable String value) {
    if (!validateNotNull(dst, field, value)) {
      return;
    }
    assert value != null;
    if (value.isEmpty()) {
      dst.add(new ValidationWarning(field, "must not be empty"));
    }
  }

  protected static <T extends Comparable<T>> void validateInRange(
      List<ValidationProblem> dst, String field, Range<T> range, @Nullable T value) {
    if (!validateNotNull(dst, field, value)) {
      return;
    }
    assert value != null;
    if (!range.contains(value)) {
      dst.add(new ValidationWarning(field, "must be in range " + range));
    }
  }
}
