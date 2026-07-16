/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import lombok.Value;

@Value
public class CompareOption {

  public static final CompareOption ASC_NULL_FIRST = new CompareOption(false, false);
  public static final CompareOption DESC_NULL_LAST = new CompareOption(true, true);

  public static final CompareOption ASC = ASC_NULL_FIRST;
  public static final CompareOption DESC = DESC_NULL_LAST;

  public static final CompareOption DEFAULT = ASC;

  boolean descending;
  boolean nullLast;

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (descending) {
      builder.append("DESC");
      if (!nullLast) {
        builder.append(" NULLS FIRST");
      }
    } else {
      builder.append("ASC");
      if (nullLast) {
        builder.append(" NULLS LAST");
      }
    }
    return builder.toString();
  }
}
