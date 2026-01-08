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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.RowStreamToBatchStreamWrapper;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;

public class BatchStreams {

  private BatchStreams() {}

  public static BatchStream wrap(
      @WillNotClose BufferAllocator allocator, @WillClose RowStream rowStream, int batchRowCount) {
    return new RowStreamToBatchStreamWrapper(allocator, rowStream, batchRowCount);
  }

  public static BatchStream empty(BatchSchema schema) {
    return new EmptyBatchStream(schema);
  }

  public static BatchStream nonColumn(int batchSize, int emptyRow) {
    return new NonColumnBatchStream(batchSize, emptyRow);
  }

  private static final EmptyBatchStream EMPTY_BATCH_STREAM =
      new EmptyBatchStream(BatchSchema.empty());

  public static BatchStream empty() {
    return EMPTY_BATCH_STREAM;
  }
}
