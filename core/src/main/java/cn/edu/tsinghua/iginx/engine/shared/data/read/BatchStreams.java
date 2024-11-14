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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.task.memory.converter.RowStreamToBatchStreamWrapper;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;

public class BatchStreams {

  private BatchStreams() {}

  public static BatchStream wrap(
      @WillNotClose BufferAllocator allocator, @WillClose RowStream rowStream, int batchRowCount) {
    return new RowStreamToBatchStreamWrapper(allocator, rowStream, batchRowCount);
  }

  public static BatchStream empty(BufferAllocator allocator, BatchSchema schema) {
    return new EmptyBatchStream(allocator, schema);
  }

  public static BatchStream empty(BufferAllocator allocator) {
    return new EmptyBatchStream(allocator, BatchSchema.empty());
  }
}
