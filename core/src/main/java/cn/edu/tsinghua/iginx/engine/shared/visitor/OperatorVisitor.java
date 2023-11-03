package cn.edu.tsinghua.iginx.engine.shared.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public interface OperatorVisitor {

  boolean needStop();

  void visit(UnaryOperator unaryOperator);

  void visit(BinaryOperator binaryOperator);

  void visit(MultipleOperator multipleOperator);
}
