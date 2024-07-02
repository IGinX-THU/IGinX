package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class OuterJoin extends AbstractJoin {

  private OuterJoinType outerJoinType;

  private Filter filter;

  private final List<String> joinColumns;

  private final boolean isNaturalJoin;

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        outerJoinType,
        filter,
        joinColumns,
        false,
        JoinAlgType.HashJoin,
        new ArrayList<>());
  }

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        outerJoinType,
        filter,
        joinColumns,
        isNaturalJoin,
        joinAlgType,
        new ArrayList<>());
  }

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.OuterJoin, sourceA, sourceB, prefixA, prefixB, joinAlgType, extraJoinPrefix);
    this.outerJoinType = outerJoinType;
    this.filter = filter;
    if (joinColumns != null) {
      this.joinColumns = joinColumns;
    } else {
      this.joinColumns = new ArrayList<>();
    }
    this.isNaturalJoin = isNaturalJoin;
  }

  public OuterJoinType getOuterJoinType() {
    return outerJoinType;
  }

  public void setOuterJoinType(OuterJoinType outerJoinType) {
    this.outerJoinType = outerJoinType;
  }

  public Filter getFilter() {
    return filter;
  }

  public List<String> getJoinColumns() {
    return joinColumns;
  }

  public boolean isNaturalJoin() {
    return isNaturalJoin;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void reChooseJoinAlg() {
    setJoinAlgType(
        JoinAlgType.chooseJoinAlg(filter, isNaturalJoin, joinColumns, getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return new OuterJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        getPrefixA(),
        getPrefixB(),
        outerJoinType,
        filter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new OuterJoin(
        sourceA,
        sourceB,
        getPrefixA(),
        getPrefixB(),
        outerJoinType,
        filter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("PrefixA: ").append(getPrefixA());
    builder.append(", PrefixB: ").append(getPrefixB());
    builder.append(", OuterJoinType: ").append(outerJoinType);
    builder.append(", IsNatural: ").append(isNaturalJoin);
    if (filter != null) {
      builder.append(", Filter: ").append(filter);
    }
    if (joinColumns != null && !joinColumns.isEmpty()) {
      builder.append(", JoinColumns: ");
      for (String col : joinColumns) {
        builder.append(col).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    if (getExtraJoinPrefix() != null && !getExtraJoinPrefix().isEmpty()) {
      builder.append(", ExtraJoinPrefix: ");
      for (String col : getExtraJoinPrefix()) {
        builder.append(col).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    OuterJoin that = (OuterJoin) object;
    return outerJoinType == that.outerJoinType
        && filter.equals(that.filter)
        && joinColumns.equals(that.joinColumns)
        && isNaturalJoin == that.isNaturalJoin
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
