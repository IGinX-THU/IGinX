package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.List;

public class OperatorInfoVisitor implements OperatorVisitor {

  private final List<Object[]> cache = new ArrayList<>();

  private int maxLen = 0;

  private int depth = -1;

  public List<Object[]> getCache() {
    return cache;
  }

  public int getMaxLen() {
    return maxLen;
  }

  @Override
  public void enter() {
    depth++;
  }

  @Override
  public void leave() {
    depth--;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    collectOperatorInfo(unaryOperator);
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    collectOperatorInfo(binaryOperator);
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    collectOperatorInfo(multipleOperator);
  }

  private void collectOperatorInfo(Operator op) {
    OperatorType type = op.getType();
    StringBuilder builder = new StringBuilder();
    if (depth != 0) {
      for (int i = 0; i < depth; i++) {
        builder.append("  ");
      }
      builder.append("+--");
    }
    builder.append(type);

    maxLen = Math.max(maxLen, builder.length());

    Object[] values = new Object[3];
    values[0] = builder.toString();
    values[1] = op.getType().toString().getBytes();
    values[2] = op.getInfo().getBytes();
    cache.add(values);
  }
}
