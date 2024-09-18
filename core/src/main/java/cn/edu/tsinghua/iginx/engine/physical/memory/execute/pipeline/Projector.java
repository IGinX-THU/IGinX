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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

public class Projector extends PipelineExecutor {

  @Override
  public String getDescription() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BatchSchema getOutputSchema(ExecutorContext context, BatchSchema inputSchema) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  protected Batch computeWithoutTimer(ExecutorContext context, Batch batch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
