package cn.edu.tsinghua.iginx.engine.shared.data.read;

import org.apache.arrow.memory.BufferAllocator;

public class BatchStreams {

  private BatchStreams() {}

  public static BatchStream wrap(BufferAllocator allocator, RowStream rowStream) {
    throw new UnsupportedOperationException("Not implemented yet");
//    return new RowStreamToBatchStreamWrapper(allocator, rowStream);
  }

  public static BatchStream empty() {
    throw new UnsupportedOperationException("Not implemented yet");
//    return new EmptyBatchStream();
  }
}
