package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;

public abstract class BinaryLogicFunction extends BinaryFunction<BitVector> {

  protected BinaryLogicFunction(String name) {
    super(name);
  }

  @Override
  protected boolean allowLeftType(Types.MinorType type) {
    return Types.MinorType.BIT == type;
  }

  @Override
  protected boolean allowRightType(Types.MinorType type) {
    return Types.MinorType.BIT == type;
  }

  @Override
  public BitVector evaluate(ExecutorContext context, @WillNotClose FieldVector left, @WillNotClose FieldVector right) {
    return evaluate(context, (BitVector) left, (BitVector) right);
  }

  public BitVector evaluate(ExecutorContext context, @WillNotClose BitVector left, @WillNotClose BitVector right) {
    BitVector result = ValueVectors.likeWithBothValidity(context.getAllocator(), left, right);
    evaluate(result.getDataBuffer(), left.getDataBuffer(), right.getDataBuffer(), BitVectorHelper.getValidityBufferSize(result.getValueCount()));
    return result;
  }

  public void evaluate(@WillNotClose ArrowBuf result, @WillNotClose ArrowBuf left, @WillNotClose ArrowBuf right, long byteCount) {
    if (result.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of result buffer is not enough");
    }
    if (left.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of left buffer is not enough");
    }
    if (right.capacity() < byteCount) {
      throw new IllegalArgumentException("The capacity of right buffer is not enough");
    }
    long countAligned = (byteCount / Long.BYTES) * Long.BYTES;
    for (long index = 0; index < countAligned; index += Long.BYTES) {
      result.setLong(index, evaluate(left.getLong(index), right.getLong(index)));
    }
    for (long index = countAligned; index < byteCount; index++) {
      result.setByte(index, evaluate(left.getByte(index), right.getByte(index)));
    }
  }

  @Override
  public void close() {

  }

  public abstract byte evaluate(byte left, byte right);

  public abstract long evaluate(long left, long right);
}
