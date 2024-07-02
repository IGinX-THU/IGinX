package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class MarkJoin extends AbstractJoin {

  public static String MARK_PREFIX = "&mark";

  private Filter filter;

  private final String markColumn;

  private final boolean isAntiJoin;

  private TagFilter tagFilter;

  public MarkJoin(
      Source sourceA,
      Source sourceB,
      Filter filter,
      String markColumn,
      boolean isAntiJoin,
      JoinAlgType joinAlgType) {
    this(sourceA, sourceB, filter, markColumn, isAntiJoin, joinAlgType, new ArrayList<>());
  }

  public MarkJoin(
      Source sourceA,
      Source sourceB,
      Filter filter,
      String markColumn,
      boolean isAntiJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.MarkJoin, sourceA, sourceB, null, null, joinAlgType, extraJoinPrefix);
    this.filter = filter;
    this.markColumn = markColumn;
    this.isAntiJoin = isAntiJoin;
  }

  public MarkJoin(
      Source sourceA,
      Source sourceB,
      Filter filter,
      TagFilter tagFilter,
      String markColumn,
      boolean isAntiJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    this(sourceA, sourceB, filter, markColumn, isAntiJoin, joinAlgType, extraJoinPrefix);
    this.tagFilter = tagFilter;
  }

  public Filter getFilter() {
    return filter;
  }

  public String getMarkColumn() {
    return markColumn;
  }

  public boolean isAntiJoin() {
    return isAntiJoin;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void reChooseJoinAlg() {
    setJoinAlgType(chooseJoinAlg(filter, false, new ArrayList<>(), getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return new MarkJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        filter.copy(),
        markColumn,
        isAntiJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new MarkJoin(
        sourceA,
        sourceB,
        filter.copy(),
        markColumn,
        isAntiJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Filter: ").append(filter.toString());
    builder.append(", MarkColumn: ").append(markColumn);
    builder.append(", IsAntiJoin: ").append(isAntiJoin);
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
    MarkJoin that = (MarkJoin) object;
    return filter.equals(that.filter)
        && markColumn.equals(that.markColumn)
        && isAntiJoin == that.isAntiJoin
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
