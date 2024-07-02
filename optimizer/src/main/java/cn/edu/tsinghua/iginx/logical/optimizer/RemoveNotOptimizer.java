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
package cn.edu.tsinghua.iginx.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveNotOptimizer implements Optimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveNotOptimizer.class);

  private static RemoveNotOptimizer instance;

  private RemoveNotOptimizer() {}

  public static RemoveNotOptimizer getInstance() {
    if (instance == null) {
      synchronized (FilterFragmentOptimizer.class) {
        if (instance == null) {
          instance = new RemoveNotOptimizer();
        }
      }
    }
    return instance;
  }

  @Override
  public Operator optimize(Operator root) {
    // only optimize query
    if (root.getType() == OperatorType.CombineNonQuery
        || root.getType() == OperatorType.ShowColumns) {
      return root;
    }

    List<Select> selectOperatorList = new ArrayList<>();
    OperatorUtils.findSelectOperators(selectOperatorList, root);

    if (selectOperatorList.isEmpty()) {
      LOGGER.info("There is no filter in logical tree.");
      return root;
    }

    for (Select selectOperator : selectOperatorList) {
      removeNot(selectOperator);
    }
    return root;
  }

  private void removeNot(Select selectOperator) {
    // remove not filter.
    Filter filter = LogicalFilterUtils.removeNot(selectOperator.getFilter());
    selectOperator.setFilter(filter);
  }
}
