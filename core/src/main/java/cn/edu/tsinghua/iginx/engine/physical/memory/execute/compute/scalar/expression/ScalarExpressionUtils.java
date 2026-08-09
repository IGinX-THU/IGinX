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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ResultRowCountException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ScalarExpressionUtils {

  private ScalarExpressionUtils() {}

  public static Field getOutputField(
      BufferAllocator allocator, ScalarExpression<?> expression, Schema inputSchema)
      throws ComputeException {
    try (VectorSchemaRoot empty = VectorSchemaRoot.create(inputSchema, allocator);
        FieldVector result =
            evaluate(allocator, ArrowDictionaries.emptyProvider(), empty, null, expression)) {
      return result.getField();
    }
  }

  public static Schema getOutputSchema(
      BufferAllocator allocator,
      List<? extends ScalarExpression<?>> expressions,
      Schema inputSchema)
      throws ComputeException {
    List<Field> fields = new ArrayList<>();
    for (ScalarExpression<?> expression : expressions) {
      fields.add(getOutputField(allocator, expression, inputSchema));
    }
    return new Schema(fields);
  }

  public static <OUTPUT extends FieldVector> OUTPUT evaluate(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection,
      ScalarExpression<OUTPUT> expression)
      throws ComputeException {
    OUTPUT result = expression.invoke(allocator, dictionaryProvider, selection, input);
    try {
      int inputCount = selection == null ? input.getRowCount() : selection.getValueCount();
      if (result.getValueCount() != inputCount) {
        throw new ResultRowCountException(
            expression, result.getField(), input.getRowCount(), result.getValueCount());
      }
    } catch (ComputeException e) {
      result.close();
      throw e;
    }
    return result;
  }

  public static VectorSchemaRoot evaluate(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection,
      List<ScalarExpression<?>> expressions)
      throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    try {
      for (ScalarExpression<?> expression : expressions) {
        results.add(evaluate(allocator, dictionaryProvider, input, selection, expression));
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return VectorSchemaRoots.create(
        results, selection == null ? input.getRowCount() : selection.getValueCount());
  }

  public static VectorSchemaRoot evaluate(
      BufferAllocator allocator, VectorSchemaRoot input, List<ScalarExpression<?>> expressions)
      throws ComputeException {
    return evaluate(allocator, ArrowDictionaries.emptyProvider(), input, null, expressions);
  }
}
