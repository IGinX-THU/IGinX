package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillCloseWhenClosed;

public interface ResultConsumer {

  void consume(@WillCloseWhenClosed CloseableDictionaryProvider dictionaryProvider, @WillCloseWhenClosed VectorSchemaRoot data);
}
