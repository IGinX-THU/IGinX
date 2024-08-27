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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    List<String> paths;
    Object[] valuesList;
    List<DataType> dataTypeList;

    paths =
        header.getFields().stream()
            .map(e -> TRANSFORM_PREFIX + "." + e.getName())
            .collect(Collectors.toList());
    dataTypeList = header.getFields().stream().map(Field::getType).collect(Collectors.toList());
    valuesList = batchData.getRowList().stream().map(Row::getValues).toArray();
    long[] keys = new long[valuesList.length];
    long startKey = System.nanoTime();
    for (int i = 0; i < valuesList.length; i++) {
      keys[i] = startKey + i;
    }

    List<Map<String, String>> tagsList =
        paths.stream().map(this::parseTags).collect(Collectors.toList());
    if (tagsList.contains(null)) {
      LOGGER.error("Unexpected error occurred during exporting.");
      return null;
    }

    List<String> sortedPaths = paths.stream().map(this::getPathName).collect(Collectors.toList());
    Integer[] index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    Object[] sortedValuesList = Arrays.copyOf(valuesList, valuesList.length);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) sortedValuesList[i])[index[j]];
      }
      sortedValuesList[i] = values;
    }
    for (Integer i : index) {
      sortedDataTypeList.add(dataTypeList.get(i));
    }
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (Integer i : index) {
      sortedTagsList.add(tagsList.get(i));
    }

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

  private Map<String, String> parseTags(String name) {
    // <string>[<string>=<string>(, <string>=<string>)*]
    String patternString =
        "^[^\\[\\]]+\\[([^\\[\\]=,]+)=([^\\[\\]=,]+)(?:, ([^\\[\\]=,]+)=([^\\[\\]=,]+))*\\]$";
    if (name.matches("^[^\\[\\]]+$")) return new HashMap<>();
    if (name.matches(patternString)) {
      Map<String, String> tagMap = new HashMap<>();
      Pattern pattern = Pattern.compile(patternString);
      Matcher matcher = pattern.matcher(name);
      for (int i = 1; i <= matcher.groupCount(); i += 2) {
        String key = matcher.group(i).trim();
        String value = matcher.group(i + 1).trim();
        tagMap.put(key, value);
      }
      return tagMap;
    }
    LOGGER.error("Invalid path format for:{}. Tags should be wrapped around by '[' & ']'.", name);
    return null;
  }

  private String getPathName(String name) {
    if (name.contains("[")) {
      return name.substring(0, name.indexOf("["));
    } else {
      return name;
    }
  }

  @Override
  public void reset() {}
}
