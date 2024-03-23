package cn.edu.tsinghua.iginx.engine.logical.constraint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintCheckerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintCheckerManager.class);

  private static final ConstraintCheckerManager instance = new ConstraintCheckerManager();

  private static final String NAIVE = "naive";

  private ConstraintCheckerManager() {}

  public static ConstraintCheckerManager getInstance() {
    return instance;
  }

  public ConstraintChecker getChecker(String name) {
    if (name == null || name.equals("")) {
      return null;
    }
    LOGGER.info("use {} as constraint checker.", name);

    switch (name) {
      case NAIVE:
        return NaiveConstraintChecker.getInstance();
      default:
        throw new IllegalArgumentException(String.format("unknown constraint checker: %s", name));
    }
  }
}
