package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import java.util.Collections;
import java.util.Set;

public class DictionaryProviders {
  private DictionaryProviders() {
  }

  private static class EmptyDictionaryProvider implements CloseableDictionaryProvider {
    @Override
    public Dictionary lookup(long id) {
      return null;
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return Collections.emptySet();
    }

    @Override
    public void close() {

    }

    @Override
    public CloseableDictionaryProvider slice(BufferAllocator allocator) {
      return this;
    }
  }

  private final static CloseableDictionaryProvider EMPTY_DICTIONARY_PROVIDER = new EmptyDictionaryProvider();

  public static DictionaryProvider empty() {
    return EMPTY_DICTIONARY_PROVIDER;
  }

  public static CloseableDictionaryProvider emptyClosable() {
    return EMPTY_DICTIONARY_PROVIDER;
  }
}
