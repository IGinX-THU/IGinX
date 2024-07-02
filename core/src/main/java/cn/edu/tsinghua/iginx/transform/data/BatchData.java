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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.utils.Constants;
import cn.edu.tsinghua.iginx.transform.utils.TypeUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;

public class BatchData {

  private final List<Row> rowList;

  private final Header header;

  private RootAllocator allocator;

  public BatchData(Header header) {
    this.rowList = new ArrayList<>();
    this.header = header;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  public void appendRow(Row row) {
    rowList.add(row);
  }

  public VectorSchemaRoot wrapAsVectorSchemaRoot() {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    List<FieldVector> vectors = new ArrayList<>();
    if (header.hasKey()) {
      vectors.add(new BigIntVector(Constants.KEY, allocator));
    }
    header
        .getFields()
        .forEach(
            field -> {
              vectors.add(
                  TypeUtils.getFieldVectorByType(field.getFullName(), field.getType(), allocator));
            });

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < rowList.size(); i++) {
      Row row = rowList.get(i);

      int colOffset = 0;
      if (header.hasKey()) {
        TypeUtils.setValue(vectors.get(colOffset), i, DataType.LONG, row.getKey());
        colOffset++;
      }

      Object[] rowData = row.getValues();
      for (int j = 0; j < rowData.length; j++) {
        TypeUtils.setValue(
            vectors.get(colOffset + j), i, header.getFields().get(j).getType(), rowData[j]);
      }
    }

    vectors.forEach(
        valueVectors -> {
          valueVectors.setValueCount(rowList.size());
          fields.add(valueVectors.getField());
        });

    return new VectorSchemaRoot(fields, vectors);
  }

  public List<Row> getRowList() {
    return rowList;
  }

  public Header getHeader() {
    return header;
  }
}
