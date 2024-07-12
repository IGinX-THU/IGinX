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
package cn.edu.tsinghua.iginx.influxdb.query.entity;

import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.influxdb.tools.SchemaTransformer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHistoryQueryRowStream implements RowStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBQueryRowStream.class);

  private final List<Map.Entry<String, List<FluxTable>>> bucketQueryResults;

  private final List<int[]> indexList;

  private final Header header;

  private int hasMoreRecords;

  private int size;

  private boolean hasCachedRow = false;

  private Row cachedRow = null;

  private Filter filter = null;

  public InfluxDBHistoryQueryRowStream(
      Map<String, List<FluxTable>> bucketQueryResults, List<String> patterns, Filter filter) {
    this.bucketQueryResults = new ArrayList<>(bucketQueryResults.entrySet());
    this.filter = filter;
    this.indexList = new ArrayList<>();
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < bucketQueryResults.size(); i++) {
      String bucket = this.bucketQueryResults.get(i).getKey();
      List<FluxTable> tables = this.bucketQueryResults.get(i).getValue();
      this.indexList.add(new int[tables.size()]);
      for (FluxTable table : tables) {
        fields.add(SchemaTransformer.toField(bucket, table));
        this.hasMoreRecords++;
        this.size++;
      }
    }

    header = new Header(Field.KEY, fields);
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    // need to do nothing
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (this.hasMoreRecords == 0) {
      return false;
    }

    try {
      if (!hasCachedRow) {
        cacheOneRow();
      }
    } catch (SQLException | PhysicalException e) {
      throw new RowFetchException(e);
    }

    return cachedRow != null;
  }

  private void cacheOneRow() throws SQLException, PhysicalException {
    if (this.hasMoreRecords == 0) {
      cachedRow = null;
      hasCachedRow = false;
      return;
    }

    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < this.bucketQueryResults.size(); i++) {
      int[] indices = indexList.get(i);
      List<FluxTable> tables = bucketQueryResults.get(i).getValue();
      for (int j = 0; j < tables.size(); j++) {
        int index = indices[j];
        FluxTable table = tables.get(j);
        List<FluxRecord> records = table.getRecords();
        if (index == records.size()) { // 数据已经消费完毕了
          continue;
        }
        FluxRecord record = records.get(index);
        timestamp = Math.min(instantToNs(record.getTime()), timestamp);
      }
    }
    if (timestamp == Long.MAX_VALUE) {
      cachedRow = null;
      hasCachedRow = false;
    }
    Object[] values = new Object[size];
    int ptr = 0;
    for (int i = 0; i < this.bucketQueryResults.size(); i++) {
      int[] indices = indexList.get(i);
      List<FluxTable> tables = bucketQueryResults.get(i).getValue();
      for (int j = 0; j < tables.size(); j++) {
        int index = indices[j];
        FluxTable table = tables.get(j);
        List<FluxRecord> records = table.getRecords();
        if (index == records.size()) { // 数据已经消费完毕了
          values[ptr++] = null;
          continue;
        }
        FluxRecord record = records.get(index);
        if (instantToNs(record.getTime()) == timestamp) {
          DataType dataType = header.getField(ptr).getType();
          Object value = record.getValue();
          if (dataType == DataType.BINARY) {
            value = ((String) value).getBytes();
          }
          values[ptr++] = value;
          indices[j]++;
          if (indices[j] == records.size()) {
            hasMoreRecords--;
          }
        } else {
          values[ptr++] = null;
        }
      }
    }
    Row row = new Row(header, timestamp, values);
    if (filter == null || FilterUtils.validate(filter, row)) {
      cachedRow = row;
      hasCachedRow = true;
    } else {
      cacheOneRow();
    }
  }

  @Override
  public Row next() throws PhysicalException {
    try {
      Row row;
      if (!hasCachedRow) {
        cacheOneRow();
      }
      row = cachedRow;
      hasCachedRow = false;
      cachedRow = null;
      return row;
    } catch (SQLException | PhysicalException e) {
      throw new RowFetchException(e);
    }
  }
}
