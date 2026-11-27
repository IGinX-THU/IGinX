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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowStreamReader extends AbstractBatchReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowStreamReader.class);

  private final RowStream rowStream;

  public RowStreamReader(RowStream rowStream, int batchSize) {
    super(batchSize);
    this.rowStream = Objects.requireNonNull(rowStream);
  }

  @Override
  protected Header readHeader() throws ReadBatchException {
    try {
      Header header = rowStream.getHeader();
      // the engine returns a header without any column for aggregations over no data
      if (header != null
          && !header.hasKey()
          && header.getFields().isEmpty()
          && !rowStream.hasNext()) {
        return Header.EMPTY_HEADER_WITH_KEY;
      }
      return header;
    } catch (PhysicalException e) {
      throw new ReadBatchException("Fail to read header of query result.", e);
    }
  }

  @Override
  protected boolean hasNextRow() throws ReadBatchException {
    try {
      return rowStream.hasNext();
    } catch (PhysicalException e) {
      throw new ReadBatchException("Fail to examine whether there is more data.", e);
    }
  }

  @Override
  protected Row nextRow() throws ReadBatchException {
    try {
      return rowStream.next();
    } catch (PhysicalException e) {
      throw new ReadBatchException("Fail to read next row of query result.", e);
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
