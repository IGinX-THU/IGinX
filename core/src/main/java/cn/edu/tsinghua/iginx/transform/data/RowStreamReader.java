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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowStreamReader implements Reader {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowStreamReader.class);

  private final RowStream rowStream;

  private final int batchSize;

  public RowStreamReader(RowStream rowStream, int batchSize) {
    this.rowStream = rowStream;
    this.batchSize = batchSize;
  }

  @Override
  public boolean hasNextBatch() {
    try {
      return rowStream.hasNext();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to examine whether there is more data, because ", e);
      return false;
    }
  }

  @Override
  public BatchData loadNextBatch() {
    try {
      BatchData batchData = new BatchData(rowStream.getHeader());
      int countDown = batchSize;
      while (countDown > 0 && rowStream.hasNext()) {
        Row row = rowStream.next();
        batchData.appendRow(row);
        countDown--;
      }
      return batchData;
    } catch (PhysicalException e) {
      LOGGER.error("Fail to load next batch of data, because ", e);
      return null;
    }
  }

  @Override
  public void close() {
    try {
      rowStream.close();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to close RowStream, because ", e);
    }
  }
}
