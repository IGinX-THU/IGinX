package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.convert.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;
import java.util.function.IntPredicate;

public abstract class ComparisonFunction extends BinaryFunction<BitVector> {

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
      try (CastNumericAsFloat8 castFunction = new CastNumericAsFloat8();
           FieldVector leftCast = castFunction.evaluate(context, left);
           FieldVector rightCast = castFunction.evaluate(context, right)) {
        return evaluateSameType(context, leftCast, rightCast);
      }
    }
    throw new ComputeException("Cannot compare " + left.getField() + " with " + right.getField());
  }

  private BitVector evaluateSameType(ExecutorContext context, FieldVector left, FieldVector right) {
    int rowCount = Math.min(left.getValueCount(), right.getValueCount());
    BitVector dest = (BitVector) ValueVectors.create(context.getAllocator(), Types.MinorType.BIT, rowCount);
    if (left instanceof NullVector || right instanceof NullVector) {
      return dest;
    }

    switch (left.getMinorType()) {
      case INT:
        evaluate(dest, (IntVector) left, (IntVector) right);
        break;
      case BIGINT:
        evaluate(dest, (BigIntVector) left, (BigIntVector) right);
        break;
      case FLOAT4:
        evaluate(dest, (Float4Vector) left, (Float4Vector) right);
        break;
      case FLOAT8:
        evaluate(dest, (Float8Vector) left, (Float8Vector) right);
        break;
      case VARBINARY:
        evaluate(dest, (VarBinaryVector) left, (VarBinaryVector) right);
      default:
        throw new IllegalStateException("Unexpected type: " + left.getMinorType());
    }
    return dest;
  }

  private void evaluate(BitVector dest, IntVector left, IntVector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, BigIntVector left, BigIntVector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, Float4Vector left, Float4Vector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, Float8Vector left, Float8Vector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, VarBinaryVector left, VarBinaryVector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private <T extends FieldVector> void genericEvaluate(BitVector dest, T left, T right, IntPredicate predicate) {
    int rowCount = dest.getValueCount();
    try (And and = new And()) {
      and.evaluate(
          dest.getValidityBuffer(),
          left.getValidityBuffer(),
          right.getValidityBuffer(),
          BitVectorHelper.getValidityBufferSize(dest.getValueCount()));
    }
    ArrowBuf destDataBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      if (!dest.isNull(i) && predicate.test(i)) {
        BitVectorHelper.setBit(destDataBuf, i);
      }
    }
  }

  @Override
  public void close() {
  }

  protected abstract boolean evaluate(int left, int right);

  protected abstract boolean evaluate(long left, long right);

  protected abstract boolean evaluate(float left, float right);

  protected abstract boolean evaluate(double left, double right);

  protected abstract boolean evaluate(byte[] left, byte[] right);

}
