package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import org.apache.arrow.vector.VectorSchemaRoot;

public class LessEqual extends ComparisonFunction {

  public static final String NAME = "less_equal";

  public LessEqual() {
    super(NAME);
  }

  @Override
  public boolean evaluate(int left, int right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(long left, long right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(float left, float right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(double left, double right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(byte[] left, byte[] right) {
    return new String(left).compareTo(new String(right)) <= 0;
  }
}
