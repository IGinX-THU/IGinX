package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;

public class MarkBuilder implements AutoCloseable {

  private final boolean reserve;
  private BitVector bitVector;
  private int index;

  public MarkBuilder() {
    this.bitVector = null;
    this.reserve = false;
  }

  public MarkBuilder(BufferAllocator allocator, String name, int capacity, boolean reserve) {
    this.bitVector = new BitVector(name, allocator);
    this.bitVector.allocateNew(capacity);
    this.reserve = reserve;
    this.index = 0;
  }

  public void appendTrue(int count) {
    if (bitVector == null) {
      return;
    }
    if (reserve) {
      doAppendFalse(count);
    } else {
      doAppendTrue(count);
    }
  }

  public void appendFalse(int count) {
    if (bitVector == null) {
      return;
    }
    if (reserve) {
      doAppendTrue(count);
    } else {
      doAppendFalse(count);
    }
  }

  private void doAppendTrue(int count) {
    ConstantVectors.setOne(bitVector.getDataBuffer(), index, count);
    index += count;
  }

  private void doAppendFalse(int count) {
    index += count;
  }

  public BitVector build(int appendNullCount) {
    if (bitVector == null) {
      return null;
    }
    bitVector.setValueCount(index + appendNullCount);
    ConstantVectors.setAllValidity(bitVector, index);
    BitVector result = bitVector;
    bitVector = null;
    return result;
  }

  @Override
  public void close() {
    if (bitVector != null) {
      bitVector.close();
    }
  }
}
