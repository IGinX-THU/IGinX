package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class FoldedOperator extends AbstractMultipleOperator {

  private final Operator incompleteRoot;

  public FoldedOperator(List<Source> sources, Operator incompleteRoot) {
    super(OperatorType.Folded, sources);
    this.incompleteRoot = incompleteRoot;
  }

  public Operator getIncompleteRoot() {
    return incompleteRoot;
  }

  @Override
  public Operator copy() {
    return new FoldedOperator(new ArrayList<>(getSources()), incompleteRoot.copy());
  }

  @Override
  public String getInfo() {
    return "";
  }
}
