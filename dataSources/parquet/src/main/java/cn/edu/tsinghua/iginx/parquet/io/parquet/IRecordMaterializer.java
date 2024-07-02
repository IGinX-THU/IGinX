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
package cn.edu.tsinghua.iginx.parquet.io.parquet;

import shaded.iginx.org.apache.parquet.io.api.GroupConverter;
import shaded.iginx.org.apache.parquet.io.api.RecordMaterializer;
import shaded.iginx.org.apache.parquet.schema.MessageType;

class IRecordMaterializer extends RecordMaterializer<IRecord> {
  private final IGroupConverter root;

  private IRecord currentRecord = null;

  public IRecordMaterializer(MessageType schema) {
    this.root =
        new IGroupConverter(
            schema,
            record -> {
              currentRecord = record;
            });
  }

  @Override
  public void skipCurrentRecord() {}

  @Override
  public IRecord getCurrentRecord() {
    return currentRecord;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
