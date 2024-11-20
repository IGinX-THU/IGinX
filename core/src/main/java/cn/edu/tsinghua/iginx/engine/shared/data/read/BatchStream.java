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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import java.util.NoSuchElementException;

public interface BatchStream extends PhysicalCloseable {

  /**
   * Get the schema of the batch stream. The schema is immutable, the caller should not modify the
   * schema. All the batches in the stream have the same schema.
   *
   * @return the schema of the batch stream
   * @throws PhysicalException if an error occurs when getting the schema
   */
  BatchSchema getSchema() throws PhysicalException;

  /**
   * Check if there is more batch in the stream.
   *
   * @return true if there is more batch in the stream, false otherwise
   * @throws PhysicalException if an error occurs when checking if there is more batch
   */
  boolean hasNext() throws PhysicalException;

  /**
   * Get the next batch in the stream. The batch is immutable, the caller should not modify the
   * batch. The caller should close the batch after using it.
   *
   * @return the next batch in the stream
   * @throws PhysicalException if an error occurs when getting the next batch
   * @throws NoSuchElementException if there is no more batch
   */
  Batch getNext() throws PhysicalException;
}
