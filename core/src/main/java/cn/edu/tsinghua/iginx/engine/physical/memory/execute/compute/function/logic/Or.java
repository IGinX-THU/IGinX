package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic;

public class Or extends BinaryLogicFunction{

  public static final String NAME = "or";

  public Or() {
    super(NAME);
  }

  @Override
  public byte evaluate(byte left, byte right) {
    return (byte) (left | right);
  }

  @Override
  public long evaluate(long left, long right) {
    return left | right;
  }
}
