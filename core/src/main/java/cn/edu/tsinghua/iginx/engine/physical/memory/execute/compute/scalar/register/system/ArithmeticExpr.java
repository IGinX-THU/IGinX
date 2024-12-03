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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.Callee;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

@AutoService(Callee.class)
public class ArithmeticExpr implements Callee {
  @Override
  public String getIdentifier() {
    return cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;
  }

  @Override
  public ScalarExpression<?> call(
      ExecutorContext context,
      Schema schema,
      List<Object> args,
      Map<String, Object> kwargs,
      List<ScalarExpression<?>> inputs)
      throws ComputeException {
    if (inputs.size() != 1) {
      throw new ComputeException("ArithmeticExpr call args size must be 1");
    }
    return inputs.get(0);
  }
}
