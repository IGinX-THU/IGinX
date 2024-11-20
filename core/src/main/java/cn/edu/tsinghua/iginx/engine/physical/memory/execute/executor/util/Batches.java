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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

public class Batches {
  private Batches() {}

  public static List<Batch> partition(
      BufferAllocator allocator, Batch batches, int partitionRowCount) {
    Preconditions.checkArgument(partitionRowCount > 0, "size must be greater than 0");
    int totalRowCount = batches.getRowCount();
    List<Batch> result = new ArrayList<>();
    for (int startIndex = 0; startIndex < totalRowCount; startIndex += partitionRowCount) {
      int sliceSize = Math.min(partitionRowCount, totalRowCount - startIndex);
      result.add(batches.slice(allocator, startIndex, sliceSize));
    }
    return result;
  }
}
