/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.iotdb.query.entity;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.iotdb.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBQueryRowStream implements RowStream {

  enum State {
    HAS_NEXT,
    NO_NEXT,
    UNKNOWN,
  }

  private static final Logger logger = LoggerFactory.getLogger(IoTDBQueryRowStream.class);

  private static final String PREFIX = "root.";

  private static final String UNIT = "unit";

  private boolean[] filterMap;

  private final SessionDataSetWrapper dataset;

  private final boolean trimStorageUnit;

  private final boolean filterByTags;

  private final Header header;

  private final Filter filter;

  private Row cachedRow;

  private State state;

  public IoTDBQueryRowStream(
      SessionDataSetWrapper dataset, boolean trimStorageUnit, Project project, Filter filter) {
    this.dataset = dataset;
    this.trimStorageUnit = trimStorageUnit;
    this.filterByTags = project.getTagFilter() != null;
    this.filter = filter;

    List<String> names = dataset.getColumnNames();
    List<String> types = dataset.getColumnTypes();

    Field time = null;
    List<Field> fields = new ArrayList<>();

    List<Boolean> filterList = new ArrayList<>();
    TagFilter tagFilter = project.getTagFilter();
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      String type = types.get(i);
      if (i == 0 && name.equals("Time")) {
        time = Field.KEY;
        continue;
      }
      name = transformColumnName(name);
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(name);
      Field field = new Field(pair.getK(), DataTypeTransformer.strFromIoTDB(type), pair.getV());
      if (!this.trimStorageUnit && field.getFullName().startsWith(UNIT)) {
        filterList.add(true);
        continue;
      }
      if (this.filterByTags && !TagKVUtils.match(pair.v, tagFilter)) {
        filterList.add(true);
        continue;
      }
      fields.add(field);
      filterList.add(false);
    }

    if (needFilter()) {
      if (time == null) {
        this.filterMap = new boolean[names.size()];
        for (int i = 0; i < names.size(); i++) {
          filterMap[i] = filterList.get(i);
        }
      } else {
        this.filterMap = new boolean[names.size() - 1];
        for (int i = 0; i < names.size() - 1; i++) {
          filterMap[i] = filterList.get(i);
        }
      }
    }

    if (time == null) {
      this.header = new Header(fields);
    } else {
      this.header = new Header(time, fields);
    }

    this.state = State.UNKNOWN;
  }

  private String transformColumnName(String columnName) {
    if (columnName.indexOf('(') != -1) {
      columnName = columnName.substring(columnName.indexOf('(') + 1, columnName.length() - 1);
    }
    if (columnName.startsWith(PREFIX)) {
      columnName =
          columnName.substring(
              columnName.indexOf('.', trimStorageUnit ? columnName.indexOf('.') + 1 : 0) + 1);
    }
    return columnName;
  }

  @Override
  public Header getHeader() {
    return this.header;
  }

  @Override
  public void close() {
    dataset.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    try {
      if (state != State.HAS_NEXT) {
        cacheOneRow();
      }
      return state == State.HAS_NEXT;
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RowFetchException(e);
    }
  }

  @Override
  public Row next() throws PhysicalException {
    try {
      Row row;
      if (state == State.UNKNOWN) {
        cacheOneRow();
      }
      row = cachedRow;
      state = State.UNKNOWN;
      cachedRow = null;
      return row;
    } catch (SQLException | PhysicalException e) {
      logger.error(e.getMessage());
      throw new RowFetchException(e);
    }
  }

  private void cacheOneRow() throws SQLException, PhysicalException {
    try {
      if (dataset.hasNext()) {
        RowRecord record = dataset.next();
        long timestamp = record.getTimestamp();
        Object[] fields = new Object[header.getFieldSize()];
        int index = 0;
        for (int i = 0; i < record.getFields().size(); i++) {
          if (needFilter() && filterMap[i]) {
            continue;
          }
          org.apache.iotdb.tsfile.read.common.Field field = record.getFields().get(i);
          if (field.getDataType() == TEXT) {
            fields[index++] = field.getBinaryV().getValues();
          } else {
            fields[index++] = field.getObjectValue(field.getDataType());
          }
        }
        state = State.HAS_NEXT;
        cachedRow = new Row(header, timestamp, fields);
        //        if (!validate(filter, cachedRow)) {
        //          cacheOneRow();
        //        }
      } else {
        state = State.NO_NEXT;
        cachedRow = null;
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      logger.error(e.getMessage());
      throw new RowFetchException(e);
    }
  }

  private boolean needFilter() {
    return !trimStorageUnit || filterByTags;
  }
}
