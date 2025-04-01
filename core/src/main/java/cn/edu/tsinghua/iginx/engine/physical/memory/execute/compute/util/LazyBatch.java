package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import lombok.Getter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import java.util.Objects;

import static org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

@Getter
public class LazyBatch implements AutoCloseable {

  private final MapDictionaryProvider dictionaryProvider;

  private final VectorSchemaRoot data;


  public static LazyBatch slice(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data) {
    return new LazyBatch(
        ArrowDictionaries.slice(allocator,dictionaryProvider,data.getSchema()),
        VectorSchemaRoots.slice(allocator, data)
    );
  }

  public LazyBatch(
      @WillCloseWhenClosed
      MapDictionaryProvider dictionaryProvider,
      @WillCloseWhenClosed
      VectorSchemaRoot data) {
    this.dictionaryProvider = Objects.requireNonNull(dictionaryProvider);
    this.data = Objects.requireNonNull(data);
  }

  @Override
  public void close(){
    data.close();
    dictionaryProvider.close();
  }

}
