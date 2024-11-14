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
package cn.edu.tsinghua.iginx.engine.shared;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {

  public static final String KEY = "key";

  public static final String ORDINAL = "ordinal";

  public static final String ALL_PATH = "*";
  public static final String ALL_PATH_SUFFIX = ".*";

  public static final String UDF_FUNC = "transform";

  public static final String WINDOW_START_COL = "window_start";
  public static final String WINDOW_END_COL = "window_end";

  // 保留列名，会在reorder时保留，并按原顺序出现在表的最前面
  public static final Set<String> RESERVED_COLS =
      new HashSet<>(Arrays.asList(WINDOW_START_COL, WINDOW_END_COL));
}
