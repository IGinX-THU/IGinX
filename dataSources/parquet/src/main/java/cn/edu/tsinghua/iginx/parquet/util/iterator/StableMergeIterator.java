package cn.edu.tsinghua.iginx.parquet.util.iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

public class StableMergeIterator<T> extends UnmodifiableIterator<T> {
  final Queue<Entry<T>> queue;

  public StableMergeIterator(
      Iterable<? extends Iterator<? extends T>> iterators, Comparator<? super T> itemComparator) {
    Comparator<Entry<T>> heapComparator = Comparator.comparing(Entry::peek, itemComparator);
    this.queue = new PriorityQueue<>(heapComparator.thenComparing(Entry::order));

    int order = 0;
    for (Iterator<? extends T> iterator : iterators) {
      if (iterator.hasNext()) {
        this.queue.add(new Entry<>(Iterators.peekingIterator(iterator), order));
        order++;
      }
    }
  }

  public boolean hasNext() {
    return !this.queue.isEmpty();
  }

  public T next() {
    Entry<T> nextEntry = this.queue.remove();
    T next = nextEntry.peekingIterator.next();
    if (nextEntry.peekingIterator.hasNext()) {
      this.queue.add(nextEntry);
    }
    return next;
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
