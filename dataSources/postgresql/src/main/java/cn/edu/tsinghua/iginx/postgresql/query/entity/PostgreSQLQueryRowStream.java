package cn.edu.tsinghua.iginx.postgresql.query.entity;

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
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.validate;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.POSTGRESQL_SEPARATOR;
import static cn.edu.tsinghua.iginx.postgresql.tools.HashUtils.toHash;
import static cn.edu.tsinghua.iginx.postgresql.tools.TagKVUtils.splitFullName;

public class PostgreSQLQueryRowStream implements RowStream {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLQueryRowStream.class);

    private final List<ResultSet> resultSets;

    private final Header header;

    private final boolean isDummy;

    private final Filter filter;

    private boolean[] gotNext; // 标记每个结果集是否已经获取到下一行，如果是，则在下次调用 next() 时无需再调用该结果集的 next()

    private long[] cachedTimestamps; // 缓存每个结果集当前的 time 列的值

    private Object[] cachedValues; // 缓存每列当前的值

    private int[] resultSetSizes; // 记录每个结果集的列数

    private Map<Field, String> fieldToColumnName; // 记录匹配 tagFilter 的列名

    private Row cachedRow;

    private boolean hasCachedRow;

    public PostgreSQLQueryRowStream(List<ResultSet> resultSets, boolean isDummy, Filter filter, TagFilter tagFilter) throws SQLException {
        this.resultSets = resultSets;
        this.isDummy = isDummy;
        this.filter = filter;

        if (resultSets.isEmpty()) {
            this.header = new Header(Field.KEY, Collections.emptyList());
            return;
        }

        boolean filterByTags = tagFilter != null;

        Field time = null;
        List<Field> fields = new ArrayList<>();
        this.resultSetSizes = new int[resultSets.size()];
        this.fieldToColumnName = new HashMap<>();

        for (int i = 0; i < resultSets.size(); i++) {
            ResultSetMetaData resultSetMetaData = resultSets.get(i).getMetaData();
            String tableName = resultSetMetaData.getTableName(1);
            int cnt = 0;
            for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
                String columnName = resultSetMetaData.getColumnName(j);
                String typeName = resultSetMetaData.getColumnTypeName(j);
                if (j == 1 && columnName.equals("time")) {
                    time = Field.KEY;
                    continue;
                }

                Pair<String, Map<String, String>> namesAndTags = splitFullName(columnName);
                Field field = new Field(
                    tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                        + namesAndTags.k.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
                    DataTypeTransformer.fromPostgreSQL(typeName),
                    namesAndTags.v
                );

                if (filterByTags && !TagKVUtils.match(namesAndTags.v, tagFilter)) {
                    continue;
                }
                fieldToColumnName.put(field, columnName);
                fields.add(field);
                cnt++;
            }
            resultSetSizes[i] = cnt;
        }

        this.header = new Header(time, fields);

        this.gotNext = new boolean[resultSets.size()];
        Arrays.fill(gotNext, false);
        this.cachedTimestamps = new long[resultSets.size()];
        Arrays.fill(cachedTimestamps, Long.MAX_VALUE);
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
        long timestamp;
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
                    long tempTimestamp;
                    Object tempValue;

                    if (isDummy) {
                        tempTimestamp = toHash(resultSet.getString("time"));
                    } else {
                        tempTimestamp = resultSet.getLong("time");
                    }
                    cachedTimestamps[i] = tempTimestamp;

                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    for (int j = 0; j < resultSetSizes[i]; j++) {
                        Object value = resultSet.getObject(fieldToColumnName.get(header.getField(startIndex + j)));
                        if (header.getField(startIndex + j).getType() == DataType.BINARY && value != null) {
                            tempValue = value.toString().getBytes();
                        } else {
                            tempValue = value;
                        }
                        cachedValues[startIndex + j] = tempValue;
                    }
                } else {
                    cachedTimestamps[i] = Long.MAX_VALUE;
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
            timestamp = Arrays.stream(cachedTimestamps).min().getAsLong();
            startIndex = 0;
            endIndex = 0;
            for (int i = 0; i < resultSets.size(); i++) {
                endIndex += resultSetSizes[i];
                if (cachedTimestamps[i] == timestamp) {
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
            cachedRow = new Row(header, timestamp, values);
            if (isDummy && !validate(filter, cachedRow)) {
                cacheOneRow();
            }
        } else {
            cachedRow = null;
        }
        hasCachedRow = true;
    }
}
