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

import java.util.Collection;
import javax.annotation.Nullable;

public class Patterns {
  private Patterns() {}

  public static boolean isAll(String pattern) {
    return pattern.equals("*");
  }

  public static boolean isAll(@Nullable Collection<String> patterns) {
    if (patterns == null) {
      return true;
    }
    return patterns.stream().anyMatch(Patterns::isAll);
  }
}
