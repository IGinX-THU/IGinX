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
