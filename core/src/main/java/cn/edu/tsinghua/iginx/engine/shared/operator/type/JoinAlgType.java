/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
