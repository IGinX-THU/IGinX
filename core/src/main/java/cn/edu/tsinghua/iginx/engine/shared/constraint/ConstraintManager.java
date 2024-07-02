package cn.edu.tsinghua.iginx.engine.shared.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface ConstraintManager {

  boolean check(Operator root);
}
