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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.register.Callee;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.temporal.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

@AutoService(Callee.class)
public class Extract implements Callee {

  @Override
  public String getIdentifier() {
    return cn.edu.tsinghua.iginx.engine.shared.function.system.Extract.EXTRACT;
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
      throw new ComputeException(
          "unexpected input path number for extract, expected 1, but got " + inputs.size());
    }
    if (args.size() != 1) {
      throw new ComputeException(
          "unexpected args number for extract, expected 1, but got " + args.size());
    }
    Object arg = args.get(0);
    if (!(arg instanceof byte[])) {
      throw new ComputeException("The arg of for extract should be a string.");
    }
    ScalarFunction<?> function;
    String functionArg = new String((byte[]) arg);
    switch (new String((byte[]) arg)) {
      case "year":
        function = new Year();
        break;
      case "month":
        function = new Month();
        break;
      case "day":
        function = new Day();
        break;
      case "hour":
        function = new Hour();
        break;
      case "minute":
        function = new Minute();
        break;
      case "second":
        function = new Second();
        break;
      default:
        throw new ComputeException(
            "The arg of for extract should be one of year, month, day, hour, minute, second, millisecond.");
    }

    Schema argumentsSchema =
        ScalarExpressions.getOutputSchema(context.getAllocator(), inputs, schema);
    String outputColumnName =
        "extract(" + argumentsSchema.getFields().get(0).getName() + ", " + functionArg + ")";
    return new CallNode<>(function, outputColumnName, inputs);
  }
}
