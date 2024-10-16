package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;
import java.util.function.IntPredicate;

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
      try (FieldVector leftCast = castFunction.evaluate(context, left);
           FieldVector rightCast = castFunction.evaluate(context, right)) {
        return evaluateSameType(context, leftCast, rightCast);
      }
    }
    throw new ComputeException("Cannot compare " + left.getField() + " with " + right.getField());
  }

  public BitVector evaluateSameType(ExecutorContext context, FieldVector left, FieldVector right) {
    BitVector result = (BitVector) ValueVectors.createWithBothValidity(context.getAllocator(), left, right, Types.MinorType.BIT);
    ArrowBuf resultBuf = result.getDataBuffer();
    switch (left.getMinorType()) {
      case INT:
        evaluate(resultBuf, (IntVector) left, (IntVector) right);
        break;
      case BIGINT:
        evaluate(resultBuf, (BigIntVector) left, (BigIntVector) right);
        break;
      case FLOAT4:
        evaluate(resultBuf, (Float4Vector) left, (Float4Vector) right);
        break;
      case FLOAT8:
        evaluate(resultBuf, (Float8Vector) left, (Float8Vector) right);
        break;
      case VARBINARY:
        evaluate(resultBuf, (VarBinaryVector) left, (VarBinaryVector) right);
      default:
        throw new IllegalStateException("Unexpected type: " + left.getMinorType());
    }
    return result;
  }

  public void evaluate(ArrowBuf resultBuf, IntVector left, IntVector right) {
    evaluateLoop(resultBuf, left.getValueCount(), index -> evaluate(left.get(index), right.get(index)));
  }

  public void evaluate(ArrowBuf resultBuf, BigIntVector left, BigIntVector right) {
    evaluateLoop(resultBuf, left.getValueCount(), index -> evaluate(left.get(index), right.get(index)));
  }

  public void evaluate(ArrowBuf resultBuf, Float4Vector left, Float4Vector right) {
    evaluateLoop(resultBuf, left.getValueCount(), index -> evaluate(left.get(index), right.get(index)));
  }

  public void evaluate(ArrowBuf resultBuf, Float8Vector left, Float8Vector right) {
    evaluateLoop(resultBuf, left.getValueCount(), index -> evaluate(left.get(index), right.get(index)));
  }

  public void evaluate(ArrowBuf resultBuf, VarBinaryVector left, VarBinaryVector right) {
    evaluateLoop(resultBuf, left.getValueCount(), index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluateLoop(ArrowBuf resultBuf, int valueCount, IntPredicate tester) {
    for (int i = 0; i < valueCount; i++) {
      if (tester.test(i)) {
        BitVectorHelper.setBit(resultBuf, i);
      }
    }
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
