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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.engine.logical.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class NaiveConstraintChecker implements ConstraintChecker {

  private static final NaiveConstraintChecker instance = new NaiveConstraintChecker();

  private NaiveConstraintChecker() {}

  public static NaiveConstraintChecker getInstance() {
    return instance;
  }

  @Override
  public boolean check(Operator root) {
    return root != null;
  }
}
