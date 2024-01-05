package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public class TreeInfoVisitor implements OperatorVisitor {

  private final StringBuilder builder = new StringBuilder();

  private int spacer = -2;

  @Override
  public void enter() {
    spacer += 2;
  }

  @Override
  public void leave() {
    spacer -= 2;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(unaryOperator.getType())
        .append("] ")
        .append(unaryOperator.getInfo())
        .append("\n");
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(binaryOperator.getType())
        .append("] ")
        .append(binaryOperator.getInfo())
        .append("\n");
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(multipleOperator.getType())
        .append("] ")
        .append(multipleOperator.getInfo())
        .append("\n");
  }

  private void makeRoomForSpacer() {
    for (int i = 0; i < spacer; i++) {
      builder.append(" ");
    }
  }

  public String getTreeInfo() {
    return builder.toString();
  }
}
