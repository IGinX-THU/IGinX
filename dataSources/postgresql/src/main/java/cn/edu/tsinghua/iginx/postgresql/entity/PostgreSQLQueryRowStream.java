package cn.edu.tsinghua.iginx.postgresql.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.postgresql.PostgreSQLStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PostgreSQLQueryRowStream implements RowStream {
    private final List<ResultSet> resultSets;
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

    private final long[] currTimestamps;

    private final Object[] currValues;

    private final Header header;
    private boolean isdummy;

    public PostgreSQLQueryRowStream(List<ResultSet> resultSets, List<Field> fields, boolean isdummy) throws SQLException {
        this.resultSets = resultSets;
        this.isdummy = isdummy;
        this.header = new Header(Field.KEY, fields);
        this.currTimestamps = new long[resultSets.size()];
        this.currValues = new Object[resultSets.size()];
        try {
            for (int i = 0; i < this.currTimestamps.length; i++) {
                ResultSet resultSet = this.resultSets.get(i);
                if (resultSet.next()) {
                    if (isdummy) {
                        this.currTimestamps[i] = toHash(resultSet.getString(1));
                    } else {
                        this.currTimestamps[i] = resultSet.getLong(1);
                    }
                    String typeName = "";
                    if (resultSet.getObject(2) != null) {
                        typeName = resultSet.getObject(2).getClass().getTypeName();
                    } else {
                        typeName = "";
                    }
                    if (typeName.contains("String")) {
                        this.currValues[i] = resultSet.getObject(2).toString().getBytes();
                    } else {
                        this.currValues[i] = resultSet.getObject(2);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // pass
        }

    }

    @Override
    public Header getHeader() {
        return this.header;
    }

    @Override
    public void close() {
        try {
            for (ResultSet resultSet : resultSets) {
                resultSet.close();
            }
        } catch (SQLException e) {
            // pass
        }
    }

    @Override
    public boolean hasNext() {
        boolean f = false;  //判断是否是全为空的行
        for (Object i : currValues) {
            if (i != null) {
                f = true;
            }
        }
        long timestamp = Long.MAX_VALUE;
        for (long currTimestamp : this.currTimestamps) {
            if (currTimestamp != Long.MIN_VALUE) {
                timestamp = Math.min(timestamp, currTimestamp);
            }
        }
        if (!f && timestamp == 0) {
            try {
                for (int i = 0; i < this.currTimestamps.length; i++) {
                    ResultSet resultSet = this.resultSets.get(i);
                    if (resultSet.next()) {
                        if (isdummy) {
                            this.currTimestamps[i] = toHash(resultSet.getString(1));
                        } else {
                            this.currTimestamps[i] = resultSet.getLong(1);
                        }
                        String typeName = "";
                        if (resultSet.getObject(2) != null) {
                            typeName = resultSet.getObject(2).getClass().getTypeName();
                        } else {
                            typeName = "null";
                        }
                        if (typeName.contains("String")) {
                            this.currValues[i] = resultSet.getObject(2).toString().getBytes();
                        } else {
                            this.currValues[i] = resultSet.getObject(2);
                        }
                    } else {
                        // 值已经取完
                        this.currTimestamps[i] = Long.MIN_VALUE;
                        this.currValues[i] = null;
                    }
                }
            } catch (Exception e) {
                //error
                logger.error("error in postgresqlrowstream ");
            }
            return hasNext();
        }

        for (long currTimestamp : this.currTimestamps) {
            if (currTimestamp != Long.MIN_VALUE) {
                return true;
            }
        }
        return false;
    }

    private long toHash(String s) {
        char c[] = s.toCharArray();
        long hv = 0;
        long base = 131;
        for (int i = 0; i < c.length; i++) {
            hv = hv * base + (long) c[i];   //利用自然数溢出，即超过 LONG_MAX 自动溢出，节省时间
        }
        if (hv < 0) {
            return -1 * hv;
        }
        return hv;
    }

    @Override
    public Row next() throws PhysicalException {
        try {
            long timestamp = Long.MAX_VALUE;
            Object[] values = new Object[this.resultSets.size()];
            for (long currTimestamp : this.currTimestamps) {
                if (currTimestamp != Long.MIN_VALUE) {
                    timestamp = Math.min(timestamp, currTimestamp);
                }
            }
            for (int i = 0; i < this.currTimestamps.length; i++) {
                if (this.currTimestamps[i] == timestamp) {
                    values[i] = this.currValues[i];
                    ResultSet resultSet = this.resultSets.get(i);
                    if (resultSet.next()) {
                        if (isdummy) {
                            this.currTimestamps[i] = toHash(resultSet.getString(1));
                        } else {
                            this.currTimestamps[i] = resultSet.getLong(1);
                        }
                        String typeName = "";
                        if (resultSet.getObject(2) != null) {
                            typeName = resultSet.getObject(2).getClass().getTypeName();
                        } else {
                            typeName = "null";
                        }
                        if (typeName.contains("String")) {
                            this.currValues[i] = resultSet.getObject(2).toString().getBytes();
                        } else {
                            this.currValues[i] = resultSet.getObject(2);
                        }
                    } else {
                        // 值已经取完
                        this.currTimestamps[i] = Long.MIN_VALUE;
                        this.currValues[i] = null;
                    }
                }
            }
            return new Row(header, timestamp, values);
        } catch (Exception e) {
            logger.info("error:", e);
            throw new RowFetchException(e);
        }
    }
}
