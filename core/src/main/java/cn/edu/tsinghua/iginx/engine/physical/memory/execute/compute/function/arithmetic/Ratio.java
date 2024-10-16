package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.types.Types;

public class Ratio extends BinaryFunction {

  public Ratio() {
    super("ratio");
  }

  @Override
  protected int evaluate(int left, int right) {
    if (right == 0) {
      throw new ArithmeticException("Divided by 0");
    }
    return left / right;
  }

  @Override
  protected long evaluate(long left, long right) {
    if (right == 0) {
      throw new ArithmeticException("Divided by 0");
    }
    return left / right;
  }

  @Override
  protected float evaluate(float left, float right) {
    if (right == 0) {
      throw new ArithmeticException("Divided by 0");
    }
    return left / right;
  }

  @Override
  protected double evaluate(double left, double right) {
    if (right == 0) {
      throw new ArithmeticException("Divided by 0");
    }
    return left / right;
  }

  @Override
  protected FieldVector invokeImpl(ExecutorContext context, FieldVector left, FieldVector right) {
    if (left instanceof NullVector || right instanceof NullVector) {
      return ConstantVectors.ofNull(context.getAllocator(), left.getValueCount());
    }
    // cast int&long to double before compute
    if (left.getMinorType() == Types.MinorType.INT
        || left.getMinorType() == Types.MinorType.BIGINT) {
      left = castFunction.evaluate(context, left);
    }
    if (right.getMinorType() == Types.MinorType.INT
        || right.getMinorType() == Types.MinorType.BIGINT) {
      right = castFunction.evaluate(context, right);
    }
    return super.invokeImpl(context, left, right);
  }
}
