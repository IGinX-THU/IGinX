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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public class CastNumericAsFloat8 extends Cast {

  public CastNumericAsFloat8() {
    super("numeric", "float8");
  }

  @Override
  public Float8Vector evaluate(ExecutorContext context, ValueVector input) {
    switch (input.getMinorType()) {
      case INT:
        return evaluate(context, (IntVector) input);
      case BIGINT:
        return evaluate(context, (BigIntVector) input);
      case FLOAT4:
        return evaluate(context, (Float4Vector) input);
      case FLOAT8:
        return evaluate(context, (Float8Vector) input);
      default:
        throw new IllegalStateException("Unexpected type: " + input.getMinorType());
    }
  }

  private Float8Vector evaluate(ExecutorContext context, IntVector input) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private Float8Vector evaluate(ExecutorContext context, BigIntVector input) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private Float8Vector evaluate(ExecutorContext context, Float4Vector input) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private Float8Vector evaluate(ExecutorContext context, Float8Vector input) {
    return ValueVectors.slice(context.getAllocator(), input);
  }

  @Override
  protected boolean allowType(int index, Types.MinorType type) {
    return Schemas.isNumeric(type);
  }
}
