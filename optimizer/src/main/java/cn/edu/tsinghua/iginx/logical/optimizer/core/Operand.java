package cn.edu.tsinghua.iginx.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public class Operand {

  private final Class<? extends Operator> clazz;

  private final List<Operand> children;

  public static final Operand ANY_OPERAND = new Operand(null, null);

  public Operand(Class<? extends Operator> clazz, List<Operand> children) {
    this.clazz = clazz;
    this.children = children;
  }

  public List<Operand> getChildren() {
    return children;
  }

  public boolean matches(Operator operator) {
    if (this == ANY_OPERAND) {
      return true;
    }
    return clazz.isInstance(operator);
  }
}
