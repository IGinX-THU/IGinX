package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public interface OperatorVisitor {

  /**
   * do sth when you enter an operator, this method will be called at the beginning of the 'accept'
   * method.
   */
  default void enter() {}

  /**
   * do sth when you leave an operator, this method will be called at the end of 'accept' method.
   */
  default void leave() {}

  /** you can stop the traverse of the operator tree early if you need. */
  default boolean needStop() {
    return false;
  }

  void visit(UnaryOperator unaryOperator);

  void visit(BinaryOperator binaryOperator);

  void visit(MultipleOperator multipleOperator);
}
