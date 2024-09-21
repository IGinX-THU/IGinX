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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public class LiteralNode extends PhysicalExpression {

  private final Value value;

  public LiteralNode(@Nullable Value value) {
    super(Collections.emptyList());
    this.value = value;
  }

  @Override
  public String getName() {
    return "literal(" + value + ")";
  }

  @Override
  public Types.MinorType getResultType(ExecutorContext context, Types.MinorType... args) {
    if (value == null) {
      return Types.MinorType.NULL;
    }
    return Schemas.toMinorType(value.getDataType());
  }

  @Override
  public ValueVector invoke(
      ExecutorContext context, int rowCount, @WillNotClose ValueVector... args) {
    return ConstantVectors.of(context.getAllocator(), value, rowCount);
  }
}
