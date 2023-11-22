package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class InnerJoin extends AbstractJoin {

  private Filter filter;

  private final List<String> joinColumns;

  private final boolean isNaturalJoin;

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns) {
    this(sourceA, sourceB, prefixA, prefixB, filter, joinColumns, false);
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        joinColumns,
        isNaturalJoin,
        JoinAlgType.HashJoin,
        new ArrayList<>());
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        joinColumns,
        isNaturalJoin,
        joinAlgType,
        new ArrayList<>());
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.InnerJoin, sourceA, sourceB, prefixA, prefixB, joinAlgType, extraJoinPrefix);
    this.filter = filter;
    if (joinColumns != null) {
      this.joinColumns = joinColumns;
    } else {
      this.joinColumns = new ArrayList<>();
    }
    this.isNaturalJoin = isNaturalJoin;
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
    setJoinAlgType(chooseJoinAlg(filter, isNaturalJoin, joinColumns, getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return new InnerJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        getPrefixA(),
        getPrefixB(),
        filter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new InnerJoin(
        sourceA,
        sourceB,
        getPrefixA(),
        getPrefixB(),
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
}
