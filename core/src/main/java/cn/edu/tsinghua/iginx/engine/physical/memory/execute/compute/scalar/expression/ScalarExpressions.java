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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ResultRowCountException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ScalarExpressions {

  private ScalarExpressions() {}

  public static Field getOutputField(
      @WillNotClose BufferAllocator allocator, ScalarExpression<?> expression, Schema inputSchema)
      throws ComputeException {
    try (VectorSchemaRoot empty = VectorSchemaRoot.create(inputSchema, allocator)) {
      try (FieldVector result = evaluateSafe(allocator, expression, empty)) {
        return result.getField();
      }
    }
  }

  public static Schema getOutputSchema(
      @WillNotClose BufferAllocator allocator,
      List<? extends ScalarExpression<?>> expressions,
      Schema inputSchema)
      throws ComputeException {
    List<Field> fields = new ArrayList<>();
    for (ScalarExpression<?> expression : expressions) {
      fields.add(getOutputField(allocator, expression, inputSchema));
    }
    return new Schema(fields);
  }

  public static <OUTPUT extends FieldVector> OUTPUT evaluateSafe(
      @WillNotClose BufferAllocator allocator,
      ScalarExpression<OUTPUT> expression,
      VectorSchemaRoot input)
      throws ComputeException {
    OUTPUT result = expression.invoke(allocator, input);
    try {
      if (result.getValueCount() != input.getRowCount()) {
        throw new ResultRowCountException(
            expression, result.getField(), input.getRowCount(), result.getValueCount());
      }
    } catch (ComputeException e) {
      result.close();
      throw e;
    }
    return result;
  }

  public static VectorSchemaRoot evaluateSafe(
      @WillNotClose BufferAllocator allocator,
      List<ScalarExpression<?>> expressions,
      VectorSchemaRoot input)
      throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    try {
      for (ScalarExpression<?> expression : expressions) {
        results.add(evaluateSafe(allocator, expression, input));
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return new VectorSchemaRoot(results);
  }
}
