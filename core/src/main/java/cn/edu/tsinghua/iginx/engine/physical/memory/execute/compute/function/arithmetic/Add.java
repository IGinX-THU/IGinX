package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

public class Add extends BinaryFunction {

  public Add() {
    super("Add");
  }

  @Override
  protected int evaluate(int left, int right) {
    return left + right;
  }

  @Override
  protected long evaluate(long left, long right) {
    return left + right;
  }

  @Override
  protected float evaluate(float left, float right) {
    return left + right;
  }

  @Override
  protected double evaluate(double left, double right) {
    return left + right;
  }
}
