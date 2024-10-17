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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor;

import org.apache.arrow.memory.BufferAllocator;

public interface ExecutorContext {

  BufferAllocator getAllocator();

  int getMaxBatchRowCount();

  void addWarningMessage(String message);

  void addProducedRowNumber(long rows);

  default void addConsumedRowNumber(int rowCount) {}

  void addCostTime(long millis);

  default void addInitializeTime(long millis) {
    addCostTime(millis);
  }

  default void addFetchTime(long millis) {
    addCostTime(millis);
  }

  default void addPipelineComputeTime(long millis) {
    addCostTime(millis);
  }

  default void addSinkConsumeTime(long millis) {
    addCostTime(millis);
  }

  default void addSinkFinishTime(long millis) {
    addCostTime(millis);
  }

  default void addSinkProduceTime(long millis) {
    addCostTime(millis);
  }
}
