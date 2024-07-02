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
package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class SizeUtils {
  private SizeUtils() {}

  private static final Map<Class<?>, Function<Object, Long>> sizeMap = new HashMap<>();

  static {
    sizeMap.put(Boolean.class, (obj) -> 1L);
    sizeMap.put(Integer.class, (obj) -> 4L);
    sizeMap.put(Long.class, (obj) -> 8L);
    sizeMap.put(Float.class, (obj) -> 4L);
    sizeMap.put(Double.class, (obj) -> 8L);
    sizeMap.put(byte[].class, (obj) -> (long) ((byte[]) obj).length);
  }

  public static long sizeOf(@Nonnull Object obj) {
    Function<Object, Long> sizeGetter = sizeMap.get(obj.getClass());
    if (sizeGetter == null) {
      throw new UnsupportedOperationException(obj.getClass() + " is not supported");
    }
    return sizeGetter.apply(obj);
  }
}
