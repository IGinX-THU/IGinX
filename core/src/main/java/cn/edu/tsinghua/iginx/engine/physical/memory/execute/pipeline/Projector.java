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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.BatchSchemaUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.table.Table;

public class Projector extends PipelineExecutor {

  private final UnaryOperator operator;

  private List<Pair<String, Integer>> columnsAndIndices; // 输出的列名和对应输入列的索引

  private BatchSchema outputSchema;

  public Projector(final UnaryOperator operator) {
    this.operator = operator;
  }

  @Override
  public String getDescription() {
    return "Projector(" + operator.getType() + "): [" + operator.getInfo() + "]";
  }

  @Override
  public void close() throws PhysicalException {}

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws PhysicalException {
    if (outputSchema != null) {
      return outputSchema;
    }
    switch (operator.getType()) {
      case Project:
        columnsAndIndices = BatchSchemaUtils.getColumnsAndIndices(inputSchema, (Project) operator);
        break;
      case Reorder:
        columnsAndIndices = BatchSchemaUtils.getColumnsAndIndices(inputSchema, (Reorder) operator);
        break;
      case Rename:
        columnsAndIndices = BatchSchemaUtils.getColumnsAndIndices(inputSchema, (Rename) operator);
        break;
      case AddSchemaPrefix:
        columnsAndIndices =
            BatchSchemaUtils.getColumnsAndIndices(inputSchema, (AddSchemaPrefix) operator);
        break;
      default:
        throw new UnexpectedOperatorException(
            "Unexpected operator type in Projector: " + operator.getType());
    }

    // 生成输出结果的BatchSchema
    BatchSchema.Builder builder = BatchSchema.builder();
    if (inputSchema.hasKey()) {
      builder.withKey();
    }
    int start = inputSchema.hasKey() ? 1 : 0;
    for (int i = start; i < columnsAndIndices.size(); i++) {
      Pair<String, Integer> pair = columnsAndIndices.get(i);
      builder.addField(pair.k, inputSchema.getFieldArrowType(pair.v), inputSchema.getTag(pair.v));
    }
    outputSchema = builder.build();
    return outputSchema;
  }

  @Override
  protected Batch internalCompute(Batch batch) throws PhysicalException {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (Pair<String, Integer> pair : columnsAndIndices) {
      fieldVectors.add(batch.raw().getVectorCopy(pair.v)); // TODO:能否不复制或少复制数据
    }
    return new Batch(new Table(fieldVectors), outputSchema);
  }
}
