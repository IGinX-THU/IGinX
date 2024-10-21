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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import java.util.function.Function;

public class DedupIterator<T> extends UnmodifiableIterator<T> {

  private final PeekingIterator<T> iterator;
  private final Function<T, ?> keyExtractor;

  public DedupIterator(Iterator<T> iterator, Function<T, ?> keyExtractor) {
    this.iterator = Iterators.peekingIterator(iterator);
    this.keyExtractor = keyExtractor;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    T next = iterator.next();
    Object key = keyExtractor.apply(next);
    while (iterator.hasNext()) {
      T peek = iterator.peek();
      Object peekKey = keyExtractor.apply(peek);
      if (!key.equals(peekKey)) {
        break;
      }
      next = peek;
      key = peekKey;
      iterator.next();
    }
    return next;
  }
}
