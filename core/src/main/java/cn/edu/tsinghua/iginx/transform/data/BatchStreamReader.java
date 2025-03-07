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
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStreamReader implements Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchStreamReader.class);

  private final BatchStream batchStream;

  public BatchStreamReader(BatchStream batchStream) {
    this.batchStream = batchStream;
  }

  @Override
  public boolean hasNextBatch() {
    try {
      return batchStream.hasNext();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to examine whether there is more data, because ", e);
      return false;
    }
  }

  @Override
  public BatchData loadNextBatch() {
    try (Batch batch = batchStream.getNext();
        VectorSchemaRoot root = batch.getData();
        ArrowReader reader = new ArrowReader(root, batch.getRowCount())) {
      // 这里ArrowReader的batchSize，和执行时batchStream的batchSize理论上应当相同，都是execute sql过程
      return reader.loadNextBatch();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to load next batch of data, because ", e);
      return null;
    }
  }

  @Override
  public void close() {
    try {
      batchStream.close();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to close batch stream.", e);
    }
  }
}
