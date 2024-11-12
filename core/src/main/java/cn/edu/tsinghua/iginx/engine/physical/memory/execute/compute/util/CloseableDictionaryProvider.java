package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public interface CloseableDictionaryProvider extends DictionaryProvider, NoExceptionAutoCloseable {
  CloseableDictionaryProvider slice(BufferAllocator allocator);
}
