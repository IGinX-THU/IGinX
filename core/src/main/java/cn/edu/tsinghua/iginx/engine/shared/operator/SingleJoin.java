package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class SingleJoin extends AbstractJoin {

  private Filter filter;

  public SingleJoin(Source sourceA, Source sourceB, Filter filter, JoinAlgType joinAlgType) {
    this(sourceA, sourceB, filter, joinAlgType, new ArrayList<>());
  }

  public SingleJoin(
      Source sourceA,
      Source sourceB,
      Filter filter,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.SingleJoin, sourceA, sourceB, null, null, joinAlgType, extraJoinPrefix);
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void reChooseJoinAlg() {
    setJoinAlgType(chooseJoinAlg(filter, false, new ArrayList<>(), getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return new SingleJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        filter.copy(),
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new SingleJoin(
        sourceA, sourceB, filter.copy(), getJoinAlgType(), new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Filter: ").append(filter.toString());
    if (getExtraJoinPrefix() != null && !getExtraJoinPrefix().isEmpty()) {
      builder.append(", ExtraJoinPrefix: ");
      for (String col : getExtraJoinPrefix()) {
        builder.append(col).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }
}
