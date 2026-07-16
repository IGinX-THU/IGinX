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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ResultRowCountException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ExpressionAccumulators {

  private ExpressionAccumulators() {}

  public static Field getOutputField(ExpressionAccumulator accumulator) throws ComputeException {
    try (FieldVector result = evaluateSafe(accumulator, Collections.emptyList())) {
      return result.getField();
    }
  }

  public static Schema getOutputSchema(List<ExpressionAccumulator> accumulators)
      throws ComputeException {
    List<Field> fields = new ArrayList<>();
    for (ExpressionAccumulator accumulator : accumulators) {
      fields.add(getOutputField(accumulator));
    }
    return new Schema(fields);
  }

  public static void update(
      List<ExpressionAccumulator> accumulators,
      List<Accumulator.State> states,
      VectorSchemaRoot input)
      throws ComputeException {
    if (accumulators.size() != states.size()) {
      throw new IllegalArgumentException("Accumulators and states must have the same size");
    }
    for (int i = 0; i < accumulators.size(); i++) {
      ExpressionAccumulator accumulator = accumulators.get(i);
      Accumulator.State state = states.get(i);
      accumulator.update(state, input);
    }
  }

  public static FieldVector evaluateSafe(
      ExpressionAccumulator accumulator, List<Accumulator.State> states) throws ComputeException {
    FieldVector result = accumulator.evaluate(states);
    try {
      if (result.getValueCount() != states.size()) {
        throw new ResultRowCountException(
            accumulator, result.getField(), states.size(), result.getValueCount());
      }
    } catch (ResultRowCountException e) {
      result.close();
      throw new ComputeException("Result row count does not match", e);
    }
    return result;
  }

  public static VectorSchemaRoot evaluateSafe(
      List<ExpressionAccumulator> accumulators, List<List<Accumulator.State>> statesColumns)
      throws ComputeException {
    if (accumulators.size() != statesColumns.size()) {
      throw new IllegalArgumentException("Accumulators and states columns must have the same size");
    }
    for (int i = 1; i < statesColumns.size(); i++) {
      if (statesColumns.get(i).size() != statesColumns.get(0).size()) {
        throw new IllegalArgumentException("All states columns must have the same size");
      }
    }
    List<FieldVector> results = new ArrayList<>();
    try {
      for (int i = 0; i < accumulators.size(); i++) {
        results.add(evaluateSafe(accumulators.get(i), statesColumns.get(i)));
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return VectorSchemaRoots.create(
        results, statesColumns.isEmpty() ? 0 : statesColumns.get(0).size());
  }
}
