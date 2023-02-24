package cn.edu.tsinghua.iginx.postgresql.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import cn.edu.tsinghua.iginx.postgresql.PostgreSQLStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLQueryRowStream implements RowStream {

  private final List<ResultSet> resultSets;
  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

  private final long[] currTimestamps;

  private final Object[] currValues;

  private final Header header;

  public PostgreSQLQueryRowStream(List<ResultSet> resultSets, List<Field> fields) {
    this.resultSets = resultSets;
    this.header = new Header(Field.KEY, fields);
    this.currTimestamps = new long[resultSets.size()];
    this.currValues = new Object[resultSets.size()];
//    this.values=new ArrayList<>();
    // 默认填充一下timestamp列表
    try {
      long j=1;
      for (int i = 0; i < this.currTimestamps.length; i++) {
        ResultSet resultSet = this.resultSets.get(i);
        if (resultSet.next()) {
          try {
            this.currTimestamps[i] = resultSet.getTimestamp(1).getTime();
          }catch (Exception e){
            this.currTimestamps[i]=j++;
          }
          this.currValues[i] = resultSet.getObject(2);
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
  public boolean hasNext() throws PhysicalException {
    for (long currTimestamp : this.currTimestamps) {
      if (currTimestamp != Long.MIN_VALUE && currTimestamp!=0) {
        return true;
      }
    }
    return false;
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
      long j=1;
      for (int i = 0; i < this.currTimestamps.length; i++) {
        if (this.currTimestamps[i] == timestamp) {
          values[i] = this.currValues[i];
          ResultSet resultSet = this.resultSets.get(i);
          if (resultSet.next()) {
            try {
              this.currTimestamps[i] = resultSet.getTimestamp(1).getTime();
            }catch (Exception e){
              logger.info("have no timestamp,set default timestamp!");
            }
            this.currValues[i] = resultSet.getObject(2);
          } else {
            // 值已经取完
            this.currTimestamps[i] = Long.MIN_VALUE;
            this.currValues[i] = null;
          }
        }
      }
      return new Row(header, timestamp, values);
    } catch (SQLException e) {
      throw new RowFetchException(e);
    }
  }
}
