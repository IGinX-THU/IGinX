package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12HistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(IoTDB12HistoryDataGenerator.class);

  private static final String CREATE_TIMESERIES = "CREATE TIMESERIES root.%s WITH DATATYPE=%s";

  private static final String INSERT_DATA = "INSERT INTO root.%s (timestamp,%s) values(%s,%s)";

  private static final Map<String, String> DATA_TYPE_MAP =
      new HashMap<String, String>() {
        {
          put("LONG", "INT64");
          put("DOUBLE", "DOUBLE");
        }
      };

  public IoTDB12HistoryDataGenerator() {}

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    try {
      Session session = new Session("127.0.0.1", port, "root", "root");
      session.open();

      for (int i = 0; i < pathList.size(); i++) {
        session.executeNonQueryStatement(
            String.format(
                CREATE_TIMESERIES,
                pathList.get(i),
                DATA_TYPE_MAP.get(dataTypeList.get(i).toString())));
      }

      int timeCnt = 0;
      for (List<Object> valueList : valuesList) {
        for (int i = 0; i < pathList.size(); i++) {
          String path = pathList.get(i);
          String deviceId = path.substring(0, path.lastIndexOf("."));
          String measurementId = path.substring(path.lastIndexOf(".") + 1);
          if (valueList.get(i) != null) {
            session.executeNonQueryStatement(
                String.format(INSERT_DATA, deviceId, measurementId, timeCnt, valueList.get(i)));
          }
        }
        timeCnt++;
      }

      session.close();
      logger.info("write data to 127.0.0.1:{} success!", port);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("write data to 127.0.0.1:{} failure: {}", port, e.getMessage());
    }
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    try {
      Session session = new Session("127.0.0.1", port, "root", "root");
      session.open();
      session.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
      session.close();
      logger.info("clear data on 127.0.0.1:{} success!", port);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.warn("clear data on 127.0.0.1:{} failure: {}", port, e.getMessage());
    }
  }
}
