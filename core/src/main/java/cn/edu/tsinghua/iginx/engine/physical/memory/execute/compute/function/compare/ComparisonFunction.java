package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;

public abstract class ComparisonFunction extends BinaryFunction<BitVector> {

  private final CastNumericAsFloat8 castFunction = new CastNumericAsFloat8();

  protected ComparisonFunction(String name) {
    super(name);
  }

  @Override
  protected boolean allowLeftType(Types.MinorType type) {
    return Schemas.isNumeric(type) || type == Types.MinorType.VARBINARY;
  }

  @Override
  protected boolean allowRightType(Types.MinorType type) {
    return Schemas.isNumeric(type) || type == Types.MinorType.VARBINARY;
  }

  @Override
  public BitVector evaluate(ExecutorContext context, @WillNotClose FieldVector left, @WillNotClose FieldVector right) throws ComputeException {
    if (left.getMinorType() == right.getMinorType()) {
      return evaluateSameType(context, left, right);
    }
    if (Schemas.isNumeric(left.getMinorType()) && Schemas.isNumeric(right.getMinorType())) {
      try (FieldVector leftCast = castFunction.evaluate(context, left); FieldVector rightCast = castFunction.evaluate(context, right)) {
        return evaluateSameType(context, leftCast, rightCast);
      }
    }
    throw new ComputeException("Cannot compare " + left.getField() + " with " + right.getField());
  }

  public BitVector evaluateSameType(ExecutorContext context, FieldVector left, FieldVector right) {
    BitVector result = (BitVector) ValueVectors.createIntersect(context.getAllocator(), left, right, Types.MinorType.BIT);
    int rowCount = left.getValueCount();
    ArrowBuf resultBuf = result.getDataBuffer();
    switch (left.getMinorType()) {
      case FLOAT8:
        Float8Vector leftFloat8 = (Float8Vector) left;
        Float8Vector rightFloat8 = (Float8Vector) right;
        for (int i = 0; i < rowCount; i++) {
          if (evaluate(leftFloat8.get(i), rightFloat8.get(i))) {
            BitVectorHelper.setBit(resultBuf, i);
          }
        }
        break;
      default:
        throw new IllegalStateException("Unexpected type: " + left.getMinorType());
    }
    return result;
  }

  @Override
  public void close() {

  }

  public abstract boolean evaluate(int left, int right);

  public abstract boolean evaluate(long left, long right);

  public abstract boolean evaluate(float left, float right);

  public abstract boolean evaluate(double left, double right);

  public abstract boolean evaluate(byte[] left, byte[] right);

}
