package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexVisitor implements OperatorVisitor {

  private final Map<Operator, Operator> parentMap = new HashMap<>();

  private final Map<Operator, List<Operator>> childrenMap = new HashMap<>();

  public Map<Operator, Operator> getParentMap() {
    return parentMap;
  }

  public Map<Operator, List<Operator>> getChildrenMap() {
    return childrenMap;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    Source source = unaryOperator.getSource();
    if (source.getType() == SourceType.Operator) {
      Operator child = ((OperatorSource) source).getOperator();
      parentMap.put(child, unaryOperator);
      childrenMap.computeIfAbsent(unaryOperator, k -> new ArrayList<>()).add(child);
    }
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    Source sourceA = binaryOperator.getSourceA();
    if (sourceA.getType() == SourceType.Operator) {
      Operator childA = ((OperatorSource) sourceA).getOperator();
      parentMap.put(childA, binaryOperator);
      childrenMap.computeIfAbsent(binaryOperator, k -> new ArrayList<>()).add(childA);
    }
    Source sourceB = binaryOperator.getSourceB();
    if (sourceB.getType() == SourceType.Operator) {
      Operator childB = ((OperatorSource) sourceB).getOperator();
      parentMap.put(childB, binaryOperator);
      childrenMap.computeIfAbsent(binaryOperator, k -> new ArrayList<>()).add(childB);
    }
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    for (Source source : multipleOperator.getSources()) {
      if (source.getType() == SourceType.Operator) {
        Operator child = ((OperatorSource) source).getOperator();
        parentMap.put(child, multipleOperator);
        childrenMap.computeIfAbsent(multipleOperator, k -> new ArrayList<>()).add(child);
      }
    }
  }
}
