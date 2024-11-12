package cn.edu.tsinghua.iginx.physical.optimizer.naive.util;

import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;

public class Joins {
  private Joins() {
  }

  public static MarkJoin reverse(MarkJoin join) {
    return new MarkJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getFilter(),
        join.getTagFilter(),
        join.getMarkColumn(),
        join.isAntiJoin(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix()
    );
  }

  public static InnerJoin reverse(InnerJoin join) {
    return new InnerJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getPrefixB(),
        join.getPrefixA(),
        join.getFilter(),
        join.getTagFilter(),
        join.getJoinColumns(),
        join.isNaturalJoin(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix()
    );
  }

  public static OuterJoin reverse(OuterJoin join) {
    return new OuterJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getPrefixB(),
        join.getPrefixA(),
        reverse(join.getOuterJoinType()),
        join.getFilter(),
        join.getJoinColumns(),
        join.isNaturalJoin(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix()
    );
  }

  public static OuterJoinType reverse(OuterJoinType type) {
    switch (type) {
      case LEFT:
        return OuterJoinType.LEFT;
      case RIGHT:
        return OuterJoinType.RIGHT;
      default:
        return type;
    }
  }

}
