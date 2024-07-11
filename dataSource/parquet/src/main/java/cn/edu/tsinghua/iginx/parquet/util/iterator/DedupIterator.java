package cn.edu.tsinghua.iginx.parquet.util.iterator;

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
