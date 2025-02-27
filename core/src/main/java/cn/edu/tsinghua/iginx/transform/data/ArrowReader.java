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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.utils.Constants;
import cn.edu.tsinghua.iginx.utils.TypeUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowReader implements Reader, AutoCloseable {

  private final VectorSchemaRoot root;

  private final int batchSize;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  public ArrowReader(VectorSchemaRoot root, int batchSize) {
    this.root = root;
    this.batchSize = batchSize;

    Schema schema = root.getSchema();
    this.header = getHeaderFromArrowSchema(schema);

    this.rowList = getRowListFromVectorSchemaRoot(root);
  }

  private Header getHeaderFromArrowSchema(Schema schema) {
    boolean hasTime = false;
    List<cn.edu.tsinghua.iginx.engine.shared.data.read.Field> fieldList = new ArrayList<>();
    for (Field field : schema.getFields()) {
      if (field.getName().equals(Constants.KEY)) {
        hasTime = true;
      } else {
        fieldList.add(
            new cn.edu.tsinghua.iginx.engine.shared.data.read.Field(
                field.getName(), TypeUtils.toDataType(field.getType())));
      }
    }

    if (hasTime) {
      return new Header(cn.edu.tsinghua.iginx.engine.shared.data.read.Field.KEY, fieldList);
    } else {
      return new Header(fieldList);
    }
  }

  private List<Row> getRowListFromVectorSchemaRoot(VectorSchemaRoot root) {
    List<Row> rowList = new ArrayList<>();

    BigIntVector bigIntVector = null;
    if (header.hasKey()) {
      bigIntVector = (BigIntVector) root.getVector(Constants.KEY);
    }

    for (int i = 0; i < root.getRowCount(); i++) {
      Object[] objects = new Object[header.getFields().size()];
      for (int j = 0; j < header.getFields().size(); j++) {
        String vectorName = header.getFields().get(j).getFullName();
        objects[j] = root.getVector(vectorName).getObject(i);
      }

      if (header.hasKey()) {
        assert bigIntVector != null;
        rowList.add(new Row(header, bigIntVector.get(i), objects));
      } else {
        rowList.add(new Row(header, objects));
      }
    }
    return rowList;
  }

  @Override
  public boolean hasNextBatch() {
    return offset < rowList.size();
  }

  @Override
  public BatchData loadNextBatch() {
    BatchData batchData = new BatchData(header);
    int countDown = batchSize;
    while (countDown > 0 && offset < rowList.size()) {
      batchData.appendRow(rowList.get(offset));
      countDown--;
      offset++;
    }
    return batchData;
  }

  @Override
  public void close() {
    rowList.clear();
    root.close();
  }
}
