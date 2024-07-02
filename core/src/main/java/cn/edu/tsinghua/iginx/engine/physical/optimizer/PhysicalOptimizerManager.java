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
