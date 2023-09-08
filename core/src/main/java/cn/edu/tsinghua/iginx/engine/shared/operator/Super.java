package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Super extends AbstractMultipleOperator {

  private final Operator rootFolded;

  public Super(List<Source> sources, Operator rootFolded) {
    super(OperatorType.Super, sources);
    this.rootFolded = rootFolded;
  }

  public Operator getRootFolded() {
    return rootFolded;
  }

  @Override
  public Operator copy() {
    return new Super(new ArrayList<>(getSources()), rootFolded.copy());
  }

  @Override
  public String getInfo() {
    return "";
  }
}
