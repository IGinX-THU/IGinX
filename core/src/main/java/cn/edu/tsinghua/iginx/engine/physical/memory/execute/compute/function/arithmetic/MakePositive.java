package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

public class MakePositive extends UnaryFunction {

  public MakePositive() {
    super("MakePositive");
  }

  @Override
  public int evaluate(int value) {
    return value;
  }

  @Override
  public long evaluate(long value) {
    return value;
  }

  @Override
  public float evaluate(float value) {
    return value;
  }

  @Override
  public double evaluate(double value) {
    return value;
  }
}
