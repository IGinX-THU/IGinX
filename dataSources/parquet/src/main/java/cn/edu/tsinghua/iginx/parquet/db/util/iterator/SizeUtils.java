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
