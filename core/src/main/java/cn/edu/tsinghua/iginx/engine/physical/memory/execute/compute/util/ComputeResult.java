package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Objects;

public class ComputeResult implements AutoCloseable {

  private CloseableDictionaryProvider dictionary;
  private VectorSchemaRoot data;

  public ComputeResult(CloseableDictionaryProvider dictionary,VectorSchemaRoot data) {
    this.data = Objects.requireNonNull(data);
    this.dictionary = Objects.requireNonNull(dictionary);
  }

  public CloseableDictionaryProvider extractDictionaryProvider() {
    CloseableDictionaryProvider result = dictionary;
    dictionary = null;
    return result;
  }

  public VectorSchemaRoot extractData() {
    VectorSchemaRoot result = data;
    data = null;
    return result;
  }

  @Override
  public void close() {
    if (dictionary != null) {
      dictionary.close();
    }
    if (data != null) {
      data.close();
    }
  }
}
