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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinCollection;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.Objects;
import javax.annotation.WillClose;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This class is used to execute hash join operation. Left is the build side, and right is the probe
 * side.
 */
public class CollectionJoinExecutor extends StatefulBinaryExecutor {

  private final JoinCollection.Builder joinCollectionBuilder;
  private final String info;
  private JoinCollection joinCollection;
  private Schema outputSchema;

  public CollectionJoinExecutor(
      ExecutorContext context,
      BatchSchema leftSchema,
      BatchSchema rightSchema,
      @WillClose JoinCollection.Builder joinCollectionBuilder,
      String info) {
    super(context, leftSchema, rightSchema, 1);
    this.joinCollectionBuilder = Objects.requireNonNull(joinCollectionBuilder);
    this.info = Objects.requireNonNull(info);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      outputSchema = joinCollectionBuilder.constructOutputSchema();
    }
    return outputSchema;
  }

  @Override
  protected String getInfo() {
    return info;
  }

  @Override
  public void close() throws ComputeException {
    joinCollectionBuilder.close();
    if (joinCollection != null) {
      joinCollection.close();
    }
    super.close();
  }

  @Override
  public boolean needConsumeRight() throws ComputeException {
    return super.needConsumeRight() && joinCollection != null;
  }

  @Override
  protected void consumeLeftUnchecked(Batch batch) throws ComputeException {
    joinCollectionBuilder.add(batch.getDictionaryProvider(), batch.getData());
  }

  @Override
  protected void consumeLeftEndUnchecked() throws ComputeException {
    joinCollection = joinCollectionBuilder.build(this::offerRawResult);
  }

  private void offerRawResult(DictionaryProvider dictionaryProvider, VectorSchemaRoot data)
      throws ComputeException {
    try (Batch batch =
        Batch.of(
            VectorSchemaRoots.slice(context.getAllocator(), data),
            ArrowDictionaries.slice(
                context.getAllocator(), dictionaryProvider, data.getSchema()))) {
      offerResult(batch);
    }
  }

  @Override
  protected void consumeRightUnchecked(Batch batch) throws ComputeException {
    joinCollection.probe(batch.getDictionaryProvider(), batch.getData());
  }

  @Override
  protected void consumeRightEndUnchecked() throws ComputeException {
    joinCollection.flush();
  }
}
