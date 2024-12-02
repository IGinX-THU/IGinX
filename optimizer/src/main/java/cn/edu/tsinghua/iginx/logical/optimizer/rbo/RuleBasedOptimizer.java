/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.rbo;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.Planner;

public class RuleBasedOptimizer implements Optimizer {

  private static final class InstanceHolder {
    static final RuleBasedOptimizer instance = new RuleBasedOptimizer();
  }

  public static RuleBasedOptimizer getInstance() {
    return InstanceHolder.instance;
  }

  @Override
  public Operator optimize(Operator root) {
    Planner planner = new RuleBasedPlanner();
    planner.setRoot(root);
    return planner.findBest();
  }
}
