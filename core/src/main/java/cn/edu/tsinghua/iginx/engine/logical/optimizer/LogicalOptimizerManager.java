package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalOptimizerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalOptimizerManager.class);

  private static final LogicalOptimizerManager instance = new LogicalOptimizerManager();

  private static final String RULE_BASE = "rbo";

  private static final String RULE_BASE_class =
      "cn.edu.tsinghua.iginx.logical.optimizer.rbo.RuleBasedOptimizer";

  private LogicalOptimizerManager() {}

  public static LogicalOptimizerManager getInstance() {
    return instance;
  }

  public Optimizer getOptimizer(String name) {
    if (name == null || name.equals("")) {
      return null;
    }
    LOGGER.info("use {} as logical optimizer.", name);
    try {
      switch (name) {
        case RULE_BASE:
          return Optimizer.class
              .getClassLoader()
              .loadClass(RULE_BASE_class)
              .asSubclass(Optimizer.class)
              .newInstance();
        default:
          throw new IllegalArgumentException(String.format("unknown logical optimizer: %s", name));
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOGGER.error("Cannot load class: {}", name, e);
    }
    return null;
  }
}
