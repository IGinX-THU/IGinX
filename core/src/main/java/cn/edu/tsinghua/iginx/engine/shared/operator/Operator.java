package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;

public interface Operator {

  void accept(OperatorVisitor visitor);

  OperatorType getType();

  Operator copy();

  String getInfo();
}
