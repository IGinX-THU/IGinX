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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import java.util.NoSuchElementException;

/**
 * Splits one complete result into batches of at most {@code batchSize} rows. A result with a valid
 * header but no rows is delivered as exactly one empty batch, so that receivers are always invoked.
 */
public abstract class AbstractBatchReader implements Reader {

  private final int batchSize;

  private Header header;

  private BatchData pendingBatch;

  private boolean delivered = false;

  private boolean exhausted = false;

  protected AbstractBatchReader(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
    }
    this.batchSize = batchSize;
  }

  /** Header of the result; must be available even if the result has no rows. */
  protected abstract Header readHeader() throws ReadBatchException;

  /** Whether another row exists, without advancing the read position. */
  protected abstract boolean hasNextRow() throws ReadBatchException;

  /** Returns the next row and advances the read position. */
  protected abstract Row nextRow() throws ReadBatchException;

  @Override
  public final boolean hasNextBatch() throws ReadBatchException {
    if (pendingBatch == null && !exhausted) {
      pendingBatch = readBatch();
      exhausted = pendingBatch == null;
    }
    return pendingBatch != null;
  }

  @Override
  public final BatchData loadNextBatch() throws ReadBatchException {
    if (!hasNextBatch()) {
      throw new NoSuchElementException("No more batch to load.");
    }
    BatchData batchData = pendingBatch;
    pendingBatch = null;
    return batchData;
  }

  private BatchData readBatch() throws ReadBatchException {
    if (header == null) {
      header = validateHeader(readHeader());
    }
    if (delivered && !hasNextRow()) {
      return null;
    }
    BatchData batchData = new BatchData(header);
    int count = 0;
    while (count < batchSize && hasNextRow()) {
      batchData.appendRow(nextRow());
      count++;
    }
    delivered = true;
    return batchData;
  }

  private static Header validateHeader(Header header) throws ReadBatchException {
    if (header == null) {
      throw new ReadBatchException("Missing header in transform data.");
    }
    if (!header.hasKey() && header.getFields().isEmpty()) {
      throw new ReadBatchException("Header of transform data must have at least one column.");
    }
    for (Field field : header.getFields()) {
      if (field.getFullName() == null || field.getFullName().isEmpty()) {
        throw new ReadBatchException("Column name in transform data must not be empty.");
      }
    }
    return header;
  }
}
