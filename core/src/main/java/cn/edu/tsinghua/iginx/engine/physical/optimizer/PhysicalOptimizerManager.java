package cn.edu.tsinghua.iginx.engine.physical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalOptimizerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalOptimizerManager.class);

  private static final String NAIVE = "naive";

  private static final String NAIVE_CLASS =
      "cn.edu.tsinghua.iginx.physical.optimizer.naive.NaivePhysicalOptimizer";

  private static final PhysicalOptimizerManager INSTANCE = new PhysicalOptimizerManager();

  private PhysicalOptimizerManager() {}

  public static PhysicalOptimizerManager getInstance() {
    return INSTANCE;
  }

  public PhysicalOptimizer getOptimizer(String name) {
    if (name == null) {
      return null;
    }
    PhysicalOptimizer optimizer = null;
    try {
      switch (name) {
        case NAIVE:
          LOGGER.info("use {} as physical optimizer.", name);
          optimizer =
              Optimizer.class
                  .getClassLoader()
                  .loadClass(NAIVE_CLASS)
                  .asSubclass(PhysicalOptimizer.class)
                  .newInstance();
          break;
        default:
          LOGGER.error("unknown physical optimizer {}, use {} as default.", name, NAIVE);
          optimizer =
              Optimizer.class
                  .getClassLoader()
                  .loadClass(NAIVE_CLASS)
                  .asSubclass(PhysicalOptimizer.class)
                  .newInstance();
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOGGER.error("Cannot load class: {}", name, e);
    }

    return optimizer;
  }
}
