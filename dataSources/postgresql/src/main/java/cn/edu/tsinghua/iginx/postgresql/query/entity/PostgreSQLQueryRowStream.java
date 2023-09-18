package cn.edu.tsinghua.iginx.postgresql.query.entity;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.validate;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.*;
import static cn.edu.tsinghua.iginx.postgresql.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.PostgreSQLSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLQueryRowStream implements RowStream {

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLQueryRowStream.class);

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

  public PostgreSQLQueryRowStream(
      List<String> databaseNameList,
      List<ResultSet> resultSets,
      boolean isDummy,
      Filter filter,
      TagFilter tagFilter)
      throws SQLException {
    this.resultSets = resultSets;
    this.isDummy = isDummy;
    this.filter = filter;

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

    for (int i = 0; i < resultSets.size(); i++) {
      ResultSetMetaData resultSetMetaData = resultSets.get(i).getMetaData();

      Set<String> columnNameSet = new HashSet<>(); // 用于检查该resultSet中是否有同名的column

      int cnt = 0;
      for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
        String tableName = resultSetMetaData.getTableName(j);
        String columnName = resultSetMetaData.getColumnName(j);
        String typeName = resultSetMetaData.getColumnTypeName(j);

        columnNameSet.add(columnName);

        if (j == 1 && columnName.equals(KEY_NAME)) {
          key = Field.KEY;
          continue;
        }

        Pair<String, Map<String, String>> namesAndTags = splitFullName(columnName);
        Field field;
        if (isDummy) {
          field =
              new Field(
                  databaseNameList.get(i) + SEPARATOR + tableName + SEPARATOR + namesAndTags.k,
                  DataTypeTransformer.fromPostgreSQL(typeName),
                  namesAndTags.v);
        } else {
          field =
              new Field(
                  tableName + SEPARATOR + namesAndTags.k,
                  DataTypeTransformer.fromPostgreSQL(typeName),
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
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public boolean hasNext() {
    if (resultSets.isEmpty()) {
      return false;
    }

    try {
      if (!hasCachedRow) {
        cacheOneRow();
      }
    } catch (SQLException | PhysicalException e) {
      logger.error(e.getMessage());
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
      logger.error(e.getMessage());
      throw new RowFetchException(e);
    }
  }

  private void cacheOneRow() throws SQLException, PhysicalException {
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
            PostgreSQLSchema schema =
                new PostgreSQLSchema(header.getField(startIndex + j).getName(), isDummy);
            String tableName = schema.getTableName();

            tableNameSet.add(tableName);

            Object value = getResultSetObject(resultSet, columnName, tableName);
            if (header.getField(startIndex + j).getType() == DataType.BINARY && value != null) {
              tempValue = value.toString().getBytes();
            } else {
              tempValue = value;
            }
            cachedValues[startIndex + j] = tempValue;
          }

          if (isDummy) {
            // 在Dummy查询的Join操作中，key列的值是由多个Join表的所有列的值拼接而成的，但实际上的Key列仅由一个表的所有列的值拼接而成
            // 所以在这里需要将key列的值截断为一个表的所有列的值，因为能合并在一行里的不同表的数据一定是key相同的
            // 所以查询出来的KEY值一定是（我们需要的KEY值 * 表的数量），因此只需要裁剪取第一个表的key列的值即可
            String keyString = resultSet.getString(KEY_NAME);
            keyString = keyString.substring(0, keyString.length() / tableNameSet.size());
            tempKey = toHash(keyString);
          } else {
            tempKey = resultSet.getLong(KEY_NAME);
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
      if (isDummy && !validate(filter, cachedRow)) {
        cacheOneRow();
      }
    } else {
      cachedRow = null;
    }
    hasCachedRow = true;
  }

  /**
   * 从结果集中获取指定column、指定table的值 不用resultSet.getObject(String
   * columnLabel)是因为：在pg的filter下推中，可能会存在column名字相同，但是table不同的情况 这时候用resultSet.getObject(String
   * columnLabel)就只能取到第一个column的值
   */
  private Object getResultSetObject(ResultSet resultSet, String columnName, String tableName) {
    try {
      if (!resultSetHasColumnWithTheSameName.get(resultSets.indexOf(resultSet))) {
        return resultSet.getObject(columnName);
      }
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
        String tempColumnName = resultSetMetaData.getColumnName(j);
        String tempTableName = resultSetMetaData.getTableName(j);
        if (tempColumnName.equals(columnName) && tempTableName.equals(tableName)) {
          return resultSet.getObject(j);
        }
      }
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }

    return null;
  }
}
