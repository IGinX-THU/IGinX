package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class CrossJoin extends AbstractJoin {

  public CrossJoin(Source sourceA, Source sourceB, String prefixA, String prefixB) {
    this(sourceA, sourceB, prefixA, prefixB, new ArrayList<>());
  }

  public CrossJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      List<String> extraJoinPrefix) {
    super(
        OperatorType.CrossJoin,
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        JoinAlgType.NestedLoopJoin,
        extraJoinPrefix);
  }

  @Override
  public Operator copy() {
    return new CrossJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        getPrefixA(),
        getPrefixB(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new CrossJoin(
        sourceA, sourceB, getPrefixA(), getPrefixB(), new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    return "PrefixA: " + getPrefixA() + ", PrefixB: " + getPrefixB();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    CrossJoin that = (CrossJoin) object;
    return getPrefixA().equals(that.getPrefixA())
        && getPrefixB().equals(that.getPrefixB())
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
