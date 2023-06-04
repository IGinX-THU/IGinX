package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12HistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(IoTDB12HistoryDataGenerator.class);

    private static final String CREATE_TIMESERIES = "CREATE TIMESERIES root.%s WITH DATATYPE=%s";

    private static final String INSERT_DATA = "INSERT INTO root.%s (timestamp,%s) values(%s,%s)";

    public IoTDB12HistoryDataGenerator() {
        this.portOri = 6667;
        this.portExp = 6668;
    }

    private void writeHistoryData(
            List<String> pathList,
            List<DataType> dataTypeList,
            List<List<Object>> valuesList,
            int port) {
        try {
            Session session = new Session("127.0.0.1", port, "root", "root");
            session.open();

            for (int i = 0; i < pathList.size(); i++) {
                session.executeNonQueryStatement(
                        String.format(
                                CREATE_TIMESERIES,
                                pathList.get(i),
                                dataTypeList.get(i).toString()));
            }

            int timeCnt = 0;
            for (List<Object> valueList : valuesList) {
                for (int i = 0; i < pathList.size(); i++) {
                    String path = pathList.get(i);
                    String deviceId = path.substring(0, path.lastIndexOf("."));
                    String measurementId = path.substring(path.lastIndexOf(".") + 1);
                    if (valueList.get(i) != null) {
                        session.executeNonQueryStatement(
                                String.format(
                                        INSERT_DATA,
                                        deviceId,
                                        measurementId,
                                        timeCnt,
                                        valueList.get(i)));
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

    public void writeHistoryDataToOri() {
        writeHistoryData(pathListOri, dataTypeListOri, valuesListOri, portOri);
    }

    public void writeHistoryDataToExp() {
        writeHistoryData(pathListExp, dataTypeListExp, valuesListExp, portExp);
    }

    public void clearData() {
        try {
            Session sessionOri = new Session("127.0.0.1", portOri, "root", "root");
            sessionOri.open();
            sessionOri.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionOri.close();

            Session sessionExp = new Session("127.0.0.1", portExp, "root", "root");
            sessionExp.open();
            sessionExp.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionExp.close();
            logger.info("clear data success!");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error("clear data failure: {}", e.getMessage());
        }
    }
}
