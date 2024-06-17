/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
