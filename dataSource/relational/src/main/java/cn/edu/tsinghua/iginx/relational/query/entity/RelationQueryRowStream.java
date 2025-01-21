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
package cn.edu.tsinghua.iginx.relational.query.entity;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.validate;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;
import static cn.edu.tsinghua.iginx.relational.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.relational.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.relational.meta.AbstractRelationalMeta;
import cn.edu.tsinghua.iginx.relational.meta.JDBCMeta;
import cn.edu.tsinghua.iginx.relational.tools.RelationSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationQueryRowStream implements RowStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(RelationQueryRowStream.class);

  private final List<ResultSet> resultSets;

  private final Header header;

  private final boolean isDummy;

  private final Filter filter;

  private boolean[] gotNext; // 标记每个结果集是否已经获取到下一行，如果是，则在下次调用 next() 时无需再调用该结果集的 next()

  private long[] cachedKeys; // 缓存每个结果集当前的 key 列的值

  private Object[] cachedValues; // 缓存每列当前的值

  private int[] resultSetSizes; // 记录每个结果集的列数

  private Map<Field, String> fieldToColumnName; // 记录匹配 tagFilter 的列名

  private Row cachedRow;

  private boolean hasCachedRow;

  private List<Boolean> resultSetHasColumnWithTheSameName;

  private List<Connection> connList;

  private AbstractRelationalMeta relationalMeta;

  private String fullKeyName = KEY_NAME;

  private boolean isPushDown = false;

  private String engine;
  private final List<List<String>> tableColumnNames;

  public RelationQueryRowStream(
      List<String> databaseNameList,
      List<ResultSet> resultSets,
      boolean isDummy,
      Filter filter,
      TagFilter tagFilter,
      List<Connection> connList,
      AbstractRelationalMeta relationalMeta)
      throws SQLException {
    this(
        databaseNameList,
        Collections.emptyList(),
        resultSets,
        isDummy,
        filter,
        tagFilter,
        connList,
        relationalMeta);
  }

  public RelationQueryRowStream(
      List<String> databaseNameList,
      List<List<String>> tableColumnNames,
      List<ResultSet> resultSets,
      boolean isDummy,
      Filter filter,
      TagFilter tagFilter,
      List<Connection> connList,
      AbstractRelationalMeta relationalMeta)
      throws SQLException {
    this.resultSets = resultSets;
    this.isDummy = isDummy;
    this.filter = filter;
    this.connList = connList;
    this.relationalMeta = relationalMeta;
    this.tableColumnNames = tableColumnNames;

    if (resultSets.isEmpty()) {
      this.header = new Header(Field.KEY, Collections.emptyList());
      return;
    }

    boolean filterByTags = tagFilter != null;

    Field key = null;
    List<Field> fields = new ArrayList<>();
    this.resultSetSizes = new int[resultSets.size()];
    this.fieldToColumnName = new HashMap<>();
    this.resultSetHasColumnWithTheSameName = new ArrayList<>();

    JDBCMeta jdbcMeta = (JDBCMeta) relationalMeta;
    this.engine = jdbcMeta.getStorageEngineMeta().getExtraParams().get("engine");
    for (int i = 0; i < resultSets.size(); i++) {
      ResultSetMetaData resultSetMetaData = resultSets.get(i).getMetaData();

      Set<String> columnNameSet = new HashSet<>(); // 用于检查该resultSet中是否有同名的column

      int cnt = 0;
      String tableName = "";
      String columnName = "";
      String typeName = "";
      int columnSize = 0;
      String columnClassName = "";
      for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
        columnName = resultSetMetaData.getColumnName(j);
        typeName = resultSetMetaData.getColumnTypeName(j);
        if (engine.equals("oracle")) {
          tableName = this.getTableName(tableColumnNames.get(i).get(j - 1));
          columnSize = resultSetMetaData.getPrecision(j);
          columnClassName = resultSetMetaData.getColumnClassName(j);
        } else {
          tableName = resultSetMetaData.getTableName(j);
        }

        if (j == 1 && columnName.contains(KEY_NAME) && columnName.contains(SEPARATOR)) {
          isPushDown = true;
        }

        if (!relationalMeta.isSupportFullJoin() && isPushDown) {
          System.out.println(columnName);
          RelationSchema relationSchema =
              new RelationSchema(columnName, isDummy, relationalMeta.getQuote());
          tableName = relationSchema.getTableName();
          columnName = relationSchema.getColumnName();
        }

        columnNameSet.add(columnName);

        if (j == 1 && columnName.equals(KEY_NAME)) {
          key = Field.KEY;
          this.fullKeyName = resultSetMetaData.getColumnName(j);
          continue;
        }

        Pair<String, Map<String, String>> namesAndTags = splitFullName(columnName);
        Field field;
        if (isDummy) {
          field =
              new Field(
                  databaseNameList.get(i) + SEPARATOR + tableName + SEPARATOR + namesAndTags.k,
                  relationalMeta
                      .getDataTypeTransformer()
                      .fromEngineType(typeName, String.valueOf(columnSize), columnClassName),
                  namesAndTags.v);
        } else {
          field =
              new Field(
                  tableName + SEPARATOR + namesAndTags.k,
                  relationalMeta
                      .getDataTypeTransformer()
                      .fromEngineType(typeName, String.valueOf(columnSize), columnClassName),
                  namesAndTags.v);
        }

        if (filterByTags && !TagKVUtils.match(namesAndTags.v, tagFilter)) {
          continue;
        }
        fieldToColumnName.put(field, columnName);
        fields.add(field);
        cnt++;
      }
      resultSetSizes[i] = cnt;

      if (columnNameSet.size() != resultSetMetaData.getColumnCount()) {
        resultSetHasColumnWithTheSameName.add(true);
      } else {
        resultSetHasColumnWithTheSameName.add(false);
      }
    }

    this.header = new Header(key, fields);

    this.gotNext = new boolean[resultSets.size()];
    Arrays.fill(gotNext, false);
    this.cachedKeys = new long[resultSets.size()];
    Arrays.fill(cachedKeys, Long.MAX_VALUE);
    this.cachedValues = new Object[fields.size()];
    Arrays.fill(cachedValues, null);
    this.cachedRow = null;
    this.hasCachedRow = false;
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public void close() {
    try {
      for (ResultSet resultSet : resultSets) {
        resultSet.close();
      }
      for (Connection conn : connList) {
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.error("error occurred when closing resultSets or connections", e);
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (resultSets.isEmpty()) {
      return false;
    }

    try {
      if (!hasCachedRow) {
        cacheOneRow();
      }
    } catch (SQLException | PhysicalException e) {
      throw new RowFetchException("unexpected error: ", e);
    }

    return cachedRow != null;
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
      throw new RowFetchException("unexpected error: ", e);
    }
  }

  private void cacheOneRow() throws SQLException, PhysicalException {
    while (true) {
      boolean hasNext = false;
      long key;
      Object[] values = new Object[header.getFieldSize()];

      int startIndex = 0;
      int endIndex = 0;
      for (int i = 0; i < resultSets.size(); i++) {
        ResultSet resultSet = resultSets.get(i);
        if (resultSetSizes[i] == 0) {
          continue;
        }
        endIndex += resultSetSizes[i];
        if (!gotNext[i]) {
          boolean tempHasNext = resultSet.next();
          hasNext |= tempHasNext;
          gotNext[i] = true;

          if (tempHasNext) {
            long tempKey;
            Object tempValue;

            Set<String> tableNameSet = new HashSet<>();

            for (int j = 0; j < resultSetSizes[i]; j++) {
              String columnName = fieldToColumnName.get(header.getField(startIndex + j));
              RelationSchema schema =
                  new RelationSchema(
                      header.getField(startIndex + j).getName(),
                      isDummy,
                      relationalMeta.getQuote());
              String tableName = schema.getTableName();

              tableNameSet.add(tableName);

              Object value = getResultSetObject(resultSet, columnName, tableName);
              if (header.getField(startIndex + j).getType() == DataType.BINARY && value != null) {
                tempValue = value.toString().getBytes();
              } else if (header.getField(startIndex + j).getType() == DataType.BOOLEAN
                  && value != null) {
                if (value instanceof Boolean) {
                  tempValue = value;
                } else {
                  if (value instanceof BigDecimal) {
                    tempValue = ((BigDecimal) value).intValue() == 1;
                  } else {
                    tempValue = ((int) value) == 1;
                  }
                }
              } else if (value instanceof BigDecimal) {
                if (header.getField(startIndex + j).getType() == DataType.INTEGER) {
                  tempValue = ((BigDecimal) value).intValue();
                } else if (header.getField(startIndex + j).getType() == DataType.FLOAT) {
                  tempValue = ((BigDecimal) value).floatValue();
                } else if (header.getField(startIndex + j).getType() == DataType.DOUBLE) {
                  tempValue = ((BigDecimal) value).doubleValue();
                } else {
                  tempValue = ((BigDecimal) value).longValue();
                }
              } else {
                tempValue = value;
              }
              cachedValues[startIndex + j] = tempValue;
            }

            if (isDummy) {
              // 在Dummy查询的Join操作中，key列的值是由多个Join表的所有列的值拼接而成的，但实际上的Key列仅由一个表的所有列的值拼接而成
              // 所以在这里需要将key列的值截断为一个表的所有列的值，因为能合并在一行里的不同表的数据一定是key相同的
              // 所以查询出来的KEY值一定是（我们需要的KEY值 * 表的数量），因此只需要裁剪取第一个表的key列的值即可
              String keyString = resultSet.getString(fullKeyName);
              keyString = keyString.substring(0, keyString.length() / tableNameSet.size());
              tempKey = toHash(keyString);
            } else {
              tempKey = resultSet.getLong(fullKeyName);
            }
            cachedKeys[i] = tempKey;

          } else {
            cachedKeys[i] = Long.MAX_VALUE;
            for (int j = startIndex; j < endIndex; j++) {
              cachedValues[j] = null;
            }
          }
        } else {
          hasNext = true;
        }
        startIndex = endIndex;
      }

      if (hasNext) {
        key = Arrays.stream(cachedKeys).min().getAsLong();
        startIndex = 0;
        endIndex = 0;
        for (int i = 0; i < resultSets.size(); i++) {
          endIndex += resultSetSizes[i];
          if (cachedKeys[i] == key) {
            for (int j = 0; j < resultSetSizes[i]; j++) {
              values[startIndex + j] = cachedValues[startIndex + j];
            }
            gotNext[i] = false;
          } else {
            for (int j = 0; j < resultSetSizes[i]; j++) {
              values[startIndex + j] = null;
            }
            gotNext[i] = true;
          }
          startIndex = endIndex;
        }
        cachedRow = new Row(header, key, values);
        if (!validate(filter, cachedRow)) {
          continue;
        }
      } else {
        cachedRow = null;
      }
      break;
    }
    hasCachedRow = true;
  }

  /**
   * 从结果集中获取指定column、指定table的值 不用resultSet.getObject(String
   * columnLabel)是因为：在pg的filter下推中，可能会存在column名字相同，但是table不同的情况 这时候用resultSet.getObject(String
   * columnLabel)就只能取到第一个column的值
   */
  private Object getResultSetObject(ResultSet resultSet, String columnName, String tableName)
      throws SQLException {
    if (!relationalMeta.isSupportFullJoin() && isPushDown) {
      return resultSet.getObject(tableName + SEPARATOR + columnName);
    }

    if (!resultSetHasColumnWithTheSameName.get(resultSets.indexOf(resultSet))) {
      return resultSet.getObject(columnName);
    }
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    if (engine.equals("oracle")) {
      int i =
          this.tableColumnNames
              .get(resultSets.indexOf(resultSet))
              .indexOf(RelationSchema.getFullName(tableName, columnName));
      // LOGGER.info("{}-{}-{}-{}-{}",resultSet.getObject(1),resultSet.getObject(2),resultSet.getObject(3),resultSet.getObject(4),resultSet.getObject(5));
      return resultSet.getObject(i + 1);
    }
    for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
      String tempColumnName = resultSetMetaData.getColumnName(j);
      String tempTableName = resultSetMetaData.getTableName(j);
      if (tempColumnName.equals(columnName) && tempTableName.equals(tableName)) {
        return resultSet.getObject(j);
      }
    }
    return null;
  }

  private String getTableName(String fullColumnName) {
    return fullColumnName.substring(0, fullColumnName.lastIndexOf(SEPARATOR));
  }
}
