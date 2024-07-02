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
