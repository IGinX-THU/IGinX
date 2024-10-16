package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

public class GreaterEqual extends ComparisonFunction {

  private static final String NAME = "greater_equal";

  protected GreaterEqual() {
    super(NAME);
  }

  @Override
  public boolean evaluate(int left, int right) {
    return left >= right;
  }

  @Override
  public boolean evaluate(long left, long right) {
    return left >= right;
  }

  @Override
  public boolean evaluate(float left, float right) {
    return left >= right;
  }

  @Override
  public boolean evaluate(double left, double right) {
    return left >= right;
  }

  @Override
  public boolean evaluate(byte[] left, byte[] right) {
    return new String(left).compareTo(new String(right)) >= 0;
  }
}
