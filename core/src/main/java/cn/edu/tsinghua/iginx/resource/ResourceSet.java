package cn.edu.tsinghua.iginx.resource;

import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;

public class ResourceSet implements AutoCloseable {

  private final BufferAllocator allocator;

  public ResourceSet(@WillCloseWhenClosed BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public void close() {
    allocator.close();
  }
}
