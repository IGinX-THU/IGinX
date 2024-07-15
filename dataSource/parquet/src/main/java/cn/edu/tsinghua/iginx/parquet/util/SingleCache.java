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
package cn.edu.tsinghua.iginx.parquet.util;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class SingleCache<T> {
  private T obj;
  private final Supplier<T> supplier;

  public SingleCache(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  public T get() {
    if (obj == null) {
      obj = supplier.get();
    }
    return obj;
  }

  public void invalidate(Consumer<T> consumer) {
    if (obj != null) {
      consumer.accept(obj);
      obj = null;
    }
  }
}
