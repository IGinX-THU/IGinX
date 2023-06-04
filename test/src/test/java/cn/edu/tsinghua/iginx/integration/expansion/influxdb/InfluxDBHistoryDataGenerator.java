package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final Logger logger =
            LoggerFactory.getLogger(InfluxDBHistoryDataGenerator.class);

    public static final String TOKEN = "testToken";

    public static final String ORI_URL = "http://localhost:8086/";

    public static final String EXP_URL = "http://localhost:8087/";

    public static final String ORGANIZATION = "testOrg";

    private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\"";

    private static final WritePrecision WRITE_PRECISION = WritePrecision.NS;

    public InfluxDBHistoryDataGenerator() {
        this.portOri = 8086;
        this.portExp = 8087;
    }

    private void writeHistoryData(
            List<String> pathList,
            List<DataType> dataTypeList,
            List<List<Object>> valuesList,
            String URL) {
        InfluxDBClient client =
                InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);
        Organization organization =
                client.getOrganizationsApi()
                        .findOrganizations()
                        .stream()
                        .filter(o -> ORGANIZATION.equals(o.getName()))
                        .findFirst()
                        .orElseThrow(IllegalAccessError::new);

        int timeCnt = 0;
        for (List<Object> valueList : valuesList) {
            for (int i = 0; i < pathList.size(); i++) {
                String path = pathList.get(i);
                DataType dataType = dataTypeList.get(i);

                String[] parts = path.split("\\.");
                String bucketName = parts[0];
                String measurementName = parts[1];
                StringBuilder fieldName = new StringBuilder();
                for (int j = 2; j < parts.length; j++) {
                    fieldName.append(parts[j]);
                    fieldName.append(".");
                }

                if (client.getBucketsApi().findBucketByName(bucketName) == null) {
                    client.getBucketsApi().createBucket(bucketName, organization);
                }

                Point point = null;
                switch (dataType) {
                    case BOOLEAN:
                        point =
                                Point.measurement(measurementName)
                                        .addField(fieldName.substring(0, fieldName.length() - 1), (boolean) valueList.get(i))
                                        .time(timeCnt, WRITE_PRECISION);
                        break;
                    case BINARY:
                        point =
                                Point.measurement(measurementName)
                                        .addField(fieldName.substring(0, fieldName.length() - 1), (String) valueList.get(i))
                                        .time(timeCnt, WRITE_PRECISION);
                        break;
                    case DOUBLE:
                        point =
                                Point.measurement(measurementName)
                                        .addField(fieldName.substring(0, fieldName.length() - 1), (Double) valueList.get(i))
                                        .time(timeCnt, WRITE_PRECISION);
                        break;
                    case INTEGER:
                        point =
                                Point.measurement(measurementName)
                                        .addField(fieldName.toString(), (Integer) valueList.get(i))
                                        .time(timeCnt, WRITE_PRECISION);
                        break;
                    default:
                        logger.error("unsupported data type: {}", dataType);
                        break;
                }
                if (point == null) {
                    break;
                }

                client.getWriteApiBlocking().writePoint(bucketName, organization.getId(), point);
            }
            timeCnt++;
        }

        client.close();
        logger.info("write data to " + URL + " success!");
    }

    public void writeHistoryDataToOri() {
        writeHistoryData(pathListOri, dataTypeListOri, valuesListOri, ORI_URL);
    }

    public void writeHistoryDataToExp() {
        writeHistoryData(pathListExp, dataTypeListExp, valuesListExp, EXP_URL);
    }

    public void clearData() {
        InfluxDBClient client =
                InfluxDBClientFactory.create(ORI_URL, TOKEN.toCharArray(), ORGANIZATION);

        client.getDeleteApi()
                .delete(
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                        String.format(DELETE_DATA, "wf01", "wt01.status"),
                        "mn",
                        ORGANIZATION);

        client.getDeleteApi()
                .delete(
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                        String.format(DELETE_DATA, "wf01", "wt01.temperature"),
                        "mn",
                        ORGANIZATION);
        client.close();
        logger.info("clear data of 127.0.0.1:8086 success!");

        client = InfluxDBClientFactory.create(EXP_URL, TOKEN.toCharArray(), ORGANIZATION);

        client.getDeleteApi()
                .delete(
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                        String.format(DELETE_DATA, "wf03", "wt01.status"),
                        "mn",
                        ORGANIZATION);

        client.getDeleteApi()
                .delete(
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                        OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                        String.format(DELETE_DATA, "wf03", "wt01.temperature"),
                        "mn",
                        ORGANIZATION);
        client.close();
        logger.info("clear data of 127.0.0.1:8087 success!");
    }
}
