package cn.edu.tsinghua.iginx.engine.shared.operator.type;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.ArrayList;
import java.util.List;

public enum JoinAlgType {
  NestedLoopJoin,
  SortedMergeJoin,
  HashJoin;

  public static JoinAlgType chooseJoinAlg(Filter filter) {
    return chooseJoinAlg(filter, false, new ArrayList<>(), new ArrayList<>());
  }

  public static JoinAlgType chooseJoinAlg(
      Filter filter, boolean isNaturalJoin, List<String> joinColumns) {
    return chooseJoinAlg(filter, isNaturalJoin, joinColumns, new ArrayList<>());
  }

  public static JoinAlgType chooseJoinAlg(
      Filter filter,
      boolean isNaturalJoin,
      List<String> joinColumns,
      List<String> extraJoinPrefix) {
    if (isNaturalJoin) {
      return HashJoin;
    }
    if (extraJoinPrefix != null && !extraJoinPrefix.isEmpty()) {
      return HashJoin;
    }
    if (joinColumns != null && !joinColumns.isEmpty()) {
      return HashJoin;
    }
    if (FilterUtils.canUseHashJoin(filter)) {
      return HashJoin;
    }
    return NestedLoopJoin;
  }
}
