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

package cn.edu.tsinghua.iginx.transform.data;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.TRANSFORM_PREFIX;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IginXWriter extends ExportWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginXWriter.class);

  private final long sessionId;

  private final StatementExecutor executor = StatementExecutor.getInstance();

  private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

  public IginXWriter(long sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  public void write(BatchData batchData) {
    InsertRowRecordsReq rowRecordsReq = buildInsertRowReq(batchData);
    if (rowRecordsReq != null) {
      RequestContext ctx = contextBuilder.build(rowRecordsReq);
      executor.execute(ctx);
    }
  }

  private InsertRowRecordsReq buildInsertRowReq(BatchData batchData) {
    Header header = batchData.getHeader();
    Object[] valuesList;

    valuesList = batchData.getRowList().stream().map(Row::getValues).toArray();
    long[] keys = new long[valuesList.length];
    long startKey = System.nanoTime();
    for (int i = 0; i < valuesList.length; i++) {
      keys[i] = startKey + i;
    }

    List<Field> sortedFields = header.getFields();
    Integer[] index = new Integer[sortedFields.size()];
    for (int i = 0; i < sortedFields.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(i -> sortedFields.get(i).getName()));
    sortedFields.sort(Comparator.comparing(Field::getName));

    Object[] sortedValuesList = Arrays.copyOf(valuesList, valuesList.length);
    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) sortedValuesList[i])[index[j]];
      }
      sortedValuesList[i] = values;
    }
    List<String> sortedPaths =
        sortedFields.stream()
            .map(e -> TRANSFORM_PREFIX + "." + e.getName())
            .collect(Collectors.toList());
    List<DataType> sortedDataTypeList =
        sortedFields.stream().map(Field::getType).collect(Collectors.toList());
    List<Map<String, String>> sortedTagsList =
        sortedFields.stream().map(Field::getTags).collect(Collectors.toList());

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      valueBufferList.add(ByteUtils.getRowByteBuffer(values, sortedDataTypeList));
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertRowRecordsReq req = new InsertRowRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(sortedPaths);
    req.setKeys(getByteArrayFromLongArray(keys));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedTagsList);
    req.setTimePrecision(TimePrecision.NS);
    return req;
  }

  @Override
  public void reset() {}
}
