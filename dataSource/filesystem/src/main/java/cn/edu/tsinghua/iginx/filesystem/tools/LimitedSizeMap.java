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
package cn.edu.tsinghua.iginx.filesystem.tools;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class LimitedSizeMap<K, V> extends LinkedHashMap<K, V> {
  private static final long serialVersionUID = 1L;
  private final int maxSize;
  private final Consumer<V> evictionListener;

  public LimitedSizeMap(int maxSize, Consumer<V> evictionListener) {
    super(maxSize + 1, 1.1f, true); // Ensure access order is true for LRU behavior
    this.maxSize = maxSize;
    this.evictionListener = evictionListener;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    boolean isSizeExceeded = size() > maxSize;
    if (isSizeExceeded) {
      V valueToRemove = eldest.getValue();
      if (evictionListener != null) {
        evictionListener.accept(valueToRemove);
      }
    }
    return isSizeExceeded;
  }
}
