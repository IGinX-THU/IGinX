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

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;
import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.influxdb.tools.TagFilterUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBQueryRowStream implements RowStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBQueryRowStream.class);

  private final Header header;

  private final List<FluxTable> tables;

  private final int[] indices;

  private int hasMoreRecords = 0;

  private List<Boolean> filterMap;

  private final boolean filterByTags;

  private List<Integer> tableFieldIndex; // 每个table在header中的起始位置

  private Filter filter;

  private boolean hasCachedRow = false;

  private Row cachedRow = null;

  public InfluxDBQueryRowStream(List<FluxTable> tables, Project project, Filter filter) {
    List<Boolean> filterList = new ArrayList<>();
    this.filterByTags = project.getTagFilter() != null;
    this.tables =
        tables.stream()
            .filter(e -> e.getRecords().size() > 0)
            .collect(Collectors.toList()); // 只保留还有数据的二维表

    this.tableFieldIndex = new ArrayList<>();
    List<Field> fields = new ArrayList<>();
    for (FluxTable table : this.tables) {
      this.tableFieldIndex.add(fields.size());
      if (!isPivotFluxTable(table)) {
        getDataFromFluxTable(project, filterList, fields, table);
      } else {
        getDataFromPivotFluxTable(project, filterList, fields, table);
      }
    }
    filterMap = filterList;
    this.header = new Header(Field.KEY, fields);
    this.indices = new int[this.tables.size()];
    this.filter = filter;
  }

  private static boolean isPivotFluxTable(FluxTable table) {
    return !(table.getColumns().get(5).getLabel().equals("_value")
        && !table.getColumns().get(5).isGroup()
        && table.getColumns().get(6).getLabel().equals("_field")
        && table.getColumns().get(6).isGroup());
  }

  private void getDataFromFluxTable(
      Project project, List<Boolean> filterList, List<Field> fields, FluxTable table) {
    Map<String, String> tags = new HashMap<>();
    String path;
    if (table.getRecords().get(0).getValueByKey("t") == null) {
      path =
          table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getField();
    } else {
      path =
          table.getRecords().get(0).getMeasurement()
              + "."
              + table.getRecords().get(0).getValueByKey(InfluxDBSchema.TAG)
              + "."
              + table.getRecords().get(0).getField();
    }
    for (int i = 8; i < table.getColumns().size(); i++) {
      String key = table.getColumns().get(i).getLabel();
      String val = (String) table.getRecords().get(0).getValueByKey(key);
      tags.put(key, val);
    }
    if (this.filterByTags && !TagFilterUtils.match(tags, project.getTagFilter())) {
      filterList.add(true);
      return;
    } else {
      this.hasMoreRecords++;
      filterList.add(false);
    }
    DataType dataType =
        fromInfluxDB(
            table.getColumns().stream()
                .filter(x -> x.getLabel().equals("_value"))
                .collect(Collectors.toList())
                .get(0)
                .getDataType());
    fields.add(new Field(path, dataType, tags));
  }

  private void getDataFromPivotFluxTable(
      Project project, List<Boolean> filterList, List<Field> fields, FluxTable table) {
    Map<String, String> tags = new HashMap<>();
    // 获取tag
    // pivot连接后的表中，tag和column会并列排放，且顺序无规律，唯一的区别在于tag的isGroup为true，而column的isGroup为false
    for (int i = 6; i < table.getColumns().size(); i++) {
      if (table.getColumns().get(i).isGroup()) {
        String key = table.getColumns().get(i).getLabel();
        String val = (String) table.getRecords().get(0).getValueByKey(key);
        tags.put(key, val);
      }
    }

    // 获取tag filter
    if (this.filterByTags && !TagFilterUtils.match(tags, project.getTagFilter())) {
      filterList.add(true);
      return;
    } else {
      this.hasMoreRecords++;
      filterList.add(false);
    }

    // 获取fields
    for (int i = 6; i < table.getColumns().size(); i++) {
      if (!table.getColumns().get(i).isGroup()) {
        // 获取path
        String path;
        if (table.getRecords().get(0).getValueByKey("t") == null) {
          path =
              table.getRecords().get(0).getMeasurement()
                  + "."
                  + table.getColumns().get(i).getLabel();
        } else {
          path =
              table.getRecords().get(0).getMeasurement()
                  + "."
                  + table.getRecords().get(0).getValueByKey(InfluxDBSchema.TAG)
                  + "."
                  + table.getColumns().get(i).getLabel();
        }

        // 获取dataType
        DataType dataType = fromInfluxDB(table.getColumns().get(i).getDataType());

        fields.add(new Field(path, dataType, tags));
      }
    }
  }

  @Override
  public Header getHeader() {
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
    for (int i = 0; i < this.tables.size(); i++) {
      if (filterMap.get(i)) {
        continue;
      }
      int index = indices[i];
      FluxTable table = this.tables.get(i);
      List<FluxRecord> records = table.getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      FluxRecord record = records.get(index);
      timestamp = Math.min(instantToNs(record.getTime()), timestamp);
    }
    if (timestamp == Long.MAX_VALUE) {
      cachedRow = null;
      hasCachedRow = false;
      return;
    }
    Object[] values = new Object[this.header.getFieldSize()];
    for (int i = 0; i < this.tables.size(); i++) {
      if (filterMap.get(i)) {
        continue;
      }
      int index = indices[i];
      FluxTable table = this.tables.get(i);
      List<FluxRecord> records = table.getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      FluxRecord record = records.get(index);

      if (instantToNs(record.getTime()) == timestamp) {
        if (!isPivotFluxTable(this.tables.get(i))) {
          DataType dataType = header.getField(i).getType();
          Object value = record.getValue();
          if (dataType == DataType.BINARY) {
            value = ((String) value).getBytes();
          }
          values[this.tableFieldIndex.get(i)] = value;
        } else {
          int tableFieldIndex = this.tableFieldIndex.get(i);
          for (int j = 6; j < table.getColumns().size(); j++) {
            // 仅取column，column的isGroup为false，tag的isGroup为true。
            if (!table.getColumns().get(j).isGroup()) {
              DataType dataType = fromInfluxDB(table.getColumns().get(j).getDataType());
              Object value = record.getValueByIndex(j);
              if (value != null && dataType == DataType.BINARY) {
                value = ((String) value).getBytes();
              }
              values[tableFieldIndex] = value;
              tableFieldIndex++;
            }
          }
        }
        indices[i]++;
        if (indices[i] == records.size()) {
          hasMoreRecords--;
        }
      }
    }
    Row row = new Row(header, timestamp, values);
    // 这里filter如果是null, 则不过滤，直接返回。
    if (filter == null || FilterUtils.validate(filter, row)) {
      cachedRow = row;
      hasCachedRow = true;
    } else {
      // 如果不满足过滤条件，继续缓存下一行，直到满足过滤条件。
      // 如果一直不满足，则会在最后一次调用时，因为hasMoreRecords为0，而返回null。
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
