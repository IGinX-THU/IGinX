package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

import javax.annotation.WillCloseWhenClosed;
import java.util.*;
import java.util.stream.LongStream;

public class ArrayDictionaryProvider implements CloseableDictionaryProvider {

  private Dictionary[] dictionaries;
  private Set<Long> dictionaryIds;

  public static ArrayDictionaryProvider of(BufferAllocator allocator, VectorSchemaRoot... batches) {
    List<FieldVector> dictionaryVectors = new ArrayList<>();
    for (VectorSchemaRoot batch : batches) {
      VectorSchemaRoot sliced = VectorSchemaRoots.slice(allocator, batch);
      dictionaryVectors.addAll(sliced.getFieldVectors());
    }
    return new ArrayDictionaryProvider(dictionaryVectors);
  }

  public ArrayDictionaryProvider(@WillCloseWhenClosed List<FieldVector> dictionaryVectors) {
    this.dictionaries = new Dictionary[dictionaryVectors.size()];
    for (int i = 0; i < dictionaryVectors.size(); i++) {
      dictionaries[i] = new Dictionary(dictionaryVectors.get(i), new DictionaryEncoding(i, false, null));
    }
    this.dictionaryIds = new ArrayIndicesSet(dictionaries.length);
  }

  private ArrayDictionaryProvider(Dictionary[] dictionaries, Set<Long> dictionaryIds) {
    this.dictionaries = dictionaries;
    this.dictionaryIds = dictionaryIds;
  }

  @Override
  public void close() {
    for (Dictionary dictionary : dictionaries) {
      dictionary.getVector().close();
    }
  }

  @Override
  public Dictionary lookup(long id) {
    if (id < 0 || id >= dictionaries.length) {
      return null;
    }
    return dictionaries[(int) id];
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return dictionaryIds;
  }

  public ArrayDictionaryProvider slice(BufferAllocator allocator) {
    Dictionary[] slicedDictionaries = new Dictionary[dictionaries.length];
    for (int i = 0; i < dictionaries.length; i++) {
      FieldVector slicedVector = ValueVectors.slice(allocator, dictionaries[i].getVector());
      slicedDictionaries[i] = new Dictionary(slicedVector, dictionaries[i].getEncoding());
    }
    return new ArrayDictionaryProvider(slicedDictionaries, dictionaryIds);
  }

  private static class ArrayIndicesSet implements Set<Long> {

    private final long length;

    public ArrayIndicesSet(int length) {
      Preconditions.checkArgument(length >= 0);
      this.length = length;
    }

    @Override
    public int size() {
      return (int) length;
    }

    @Override
    public boolean isEmpty() {
      return length == 0;
    }

    @Override
    public boolean contains(Object o) {
      return o instanceof Long && (long) o >= 0 && (long) o < length;
    }

    @Override
    public Iterator<Long> iterator() {
      return LongStream.range(0, length).iterator();
    }

    @Override
    public Object[] toArray() {
      return LongStream.range(0, length).boxed().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return LongStream.range(0, length).boxed().toArray(i -> (T[]) new Long[(int) length]);
    }

    @Override
    public boolean add(Long aLong) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return c.stream().allMatch(this::contains);
    }

    @Override
    public boolean addAll(Collection<? extends Long> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }
}
