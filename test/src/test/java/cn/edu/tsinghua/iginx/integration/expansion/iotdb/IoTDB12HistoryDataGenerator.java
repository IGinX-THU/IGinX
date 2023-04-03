package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12HistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(IoTDB12HistoryDataGenerator.class);
    private String INSERT = "INSERT INTO root.%s (timestamp,%s) values(%s,%s)";

    @Test
    public void clearData() {
        try {
            Session sessionA = new Session("127.0.0.1", 6667, "root", "root");
            sessionA.open();
            sessionA.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionA.close();

            Session sessionB = new Session("127.0.0.1", 6668, "root", "root");
            sessionB.open();
            sessionB.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionB.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info("clear data success!");
    }

    private void writeHistoryData(
            Map<String, Pair<DataType, List<Pair<Long, Object>>>> series, int port)
            throws Exception {
        Session session = new Session("127.0.0.1", port, "root", "root");
        session.open();

        series.entrySet()
                .stream()
                .forEach(
                        entry -> {
                            String key = entry.getKey();
                            Pair<DataType, List<Pair<Long, Object>>> value = entry.getValue();
                            String p2 = key.substring(key.lastIndexOf(".") + 1);
                            String p1 = key.substring(0, key.lastIndexOf("."));
                            List<Pair<Long, Object>> valList = value.v;
                            DataType type = value.k;
                            for (Pair<Long, Object> val : valList) {
                                try {
                                    session.executeNonQueryStatement(
                                            String.format(
                                                    INSERT, p1, p2, String.valueOf(val.k), val.v));
                                } catch (IoTDBConnectionException | StatementExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        session.close();
        logger.info("write data to 127.0.0.1:" + port + "success!");
    }

    @Test
    public void writeHistoryDataToA() throws Exception {
        writeHistoryData(this.seriesA, 6667);
    }

    @Test
    public void writeHistoryDataToB() throws Exception {
        writeHistoryData(this.seriesB, 6668);
    }
}
