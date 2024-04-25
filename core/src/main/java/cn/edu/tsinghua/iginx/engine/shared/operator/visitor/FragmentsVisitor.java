package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

public class FragmentsVisitor implements OperatorVisitor {

  private int fragmentCount = 0;

  public int getFragmentCount() {
    return fragmentCount;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    if (unaryOperator.getSource().getType().equals(SourceType.Fragment)) {
      fragmentCount++;
    }
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    // do nothing
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    // do nothing
  }
}
