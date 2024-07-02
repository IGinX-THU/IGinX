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
