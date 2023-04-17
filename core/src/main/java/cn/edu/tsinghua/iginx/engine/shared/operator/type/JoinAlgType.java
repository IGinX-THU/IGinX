package cn.edu.tsinghua.iginx.engine.shared.operator.type;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.canUseHashJoin;

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
        if (extraJoinPrefix != null) {
            if (!extraJoinPrefix.isEmpty()) {
                return HashJoin;
            }
        }
        if (joinColumns != null) {
            if (!joinColumns.isEmpty()) {
                return HashJoin;
            }
        }
        if (canUseHashJoin(filter)) {
            return HashJoin;
        }
        return NestedLoopJoin;
    }
}
