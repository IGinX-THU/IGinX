package cn.edu.tsinghua.iginx.engine.logical.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class NaiveConstraintChecker implements ConstraintChecker {

  private static final NaiveConstraintChecker instance = new NaiveConstraintChecker();

  private NaiveConstraintChecker() {}

  public static NaiveConstraintChecker getInstance() {
    return instance;
  }

  @Override
  public boolean check(Operator root) {
    return root != null;
  }
}
