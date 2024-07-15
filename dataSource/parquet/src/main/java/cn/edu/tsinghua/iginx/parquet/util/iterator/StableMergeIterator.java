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
package cn.edu.tsinghua.iginx.parquet.util.iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class StableMergeIterator<T> extends UnmodifiableIterator<T> {
  private final Comparator<Entry<T>> comparator;
  private final PriorityQueue<Entry<T>> queue;
  private Entry<T> activeEntry;

  public StableMergeIterator(
      Iterable<? extends Iterator<? extends T>> iterators, Comparator<? super T> itemComparator) {

    Comparator<Entry<T>> heapComparator = Comparator.comparing(Entry::peek, itemComparator);
    this.comparator = heapComparator.thenComparing(Entry::order);
    this.queue = new PriorityQueue<>(comparator);

    int order = 0;
    for (Iterator<? extends T> iterator : iterators) {
      if (iterator.hasNext()) {
        this.queue.add(new Entry<>(Iterators.peekingIterator(iterator), order));
        order++;
      }
    }
    if (!this.queue.isEmpty()) {
      this.activeEntry = this.queue.remove();
    }
  }

  public boolean hasNext() {
    return activeEntry != null;
  }

  public T next() {
    T next = activeEntry.peekingIterator.next();
    refreshActiveEntry();
    return next;
  }

  private void refreshActiveEntry() {
    if (!activeEntry.peekingIterator.hasNext()) {
      activeEntry = this.queue.poll();
      return;
    }
    if (!queue.isEmpty()) {
      Entry<T> nextEntry = queue.peek();
      if (comparator.compare(activeEntry, nextEntry) > 0) {
        queue.add(activeEntry);
        activeEntry = queue.poll();
      }
    }
  }

  private static class Entry<T> {
    final PeekingIterator<T> peekingIterator;
    final int order;

    static <T> T peek(Entry<T> entry) {
      return entry.peekingIterator.peek();
    }

    static <T> int order(Entry<T> entry) {
      return entry.order;
    }

    private Entry(PeekingIterator<T> peekingIterator, int order) {
      this.peekingIterator = peekingIterator;
      this.order = order;
    }
  }
}
