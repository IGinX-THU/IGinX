package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHistoryDataGenerator extends BaseHistoryDataGenerator {

    private static final Logger logger =
            LoggerFactory.getLogger(InfluxDBHistoryDataGenerator.class);

    public static final String TOKEN = "testToken";

    public static final String URL = "http://localhost:8086/";

    public static final String URL2 = "http://localhost:8087/";

    public static final String ORGANIZATION = "testOrg";

    private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\"";

    private static final WritePrecision WRITE_PRECISION = WritePrecision.NS;

    private void writeHistoryData(
            Map<String, Pair<DataType, List<Pair<Long, Object>>>> series, String URL)
            throws Exception {
        InfluxDBClient client =
                InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);
        Organization organization =
                client.getOrganizationsApi()
                        .findOrganizations()
                        .stream()
                        .filter(o -> ORGANIZATION.equals(o.getName()))
                        .findFirst()
                        .orElseThrow(IllegalAccessError::new);

        series.entrySet()
                .stream()
                .forEach(
                        entry -> {
                            String key = entry.getKey();
                            Pair<DataType, List<Pair<Long, Object>>> value = entry.getValue();
                            String[] parts = key.split("\\.");
                            String p1 = parts[0];
                            String p2 = parts[1];
                            String p3 = new String();
                            for (int i = 2; i < parts.length; i++) {
                                p3 += parts[i] + ".";
                            }
                            p3 = p3.substring(0, p3.length() - 1);

                            if (client.getBucketsApi().findBucketByName(p1) == null)
                                client.getBucketsApi().createBucket(p1, organization);
                            List<Point> points = new ArrayList<>();

                            List<Pair<Long, Object>> valList = value.v;
                            DataType type = value.k;

                            for (Pair<Long, Object> val : valList) {
                                switch (type) {
                                    case BOOLEAN:
                                        points.add(
                                                Point.measurement(p2)
                                                        .addField(p3, (boolean) val.v)
                                                        .time(val.k, WRITE_PRECISION));
                                        break;
                                    case BINARY:
                                        points.add(
                                                Point.measurement(p2)
                                                        .addField(p3, (String) val.v)
                                                        .time(val.k, WRITE_PRECISION));
                                        break;
                                    case DOUBLE:
                                        points.add(
                                                Point.measurement(p2)
                                                        .addField(p3, (Double) val.v)
                                                        .time(val.k, WRITE_PRECISION));
                                        break;
                                    case INTEGER:
                                        points.add(
                                                Point.measurement(p2)
                                                        .addField(p3, (Integer) val.v)
                                                        .time(val.k, WRITE_PRECISION));
                                        break;
                                    default:
                                        logger.error("not support data type: {}", type);
                                        break;
                                }
                            }
                            client.getWriteApiBlocking()
                                    .writePoints(p1, organization.getId(), points);
                        });

        client.close();
        logger.info("write data to " + URL + " success!");
    }

    @Test
    public void writeHistoryDataToA() throws Exception {
        writeHistoryData(seriesA, URL);
    }

    @Test
    public void writeHistoryDataToB() throws Exception {
        writeHistoryData(seriesB, URL2);
    }

    @Test
    public void clearData() {
        try {
            InfluxDBClient client =
                    InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);

            client.getDeleteApi()
                    .delete(
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                            String.format(DELETE_DATA, "wf01", "wt01.status"),
                            "ln",
                            ORGANIZATION);

            client.getDeleteApi()
                    .delete(
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                            String.format(DELETE_DATA, "wf01", "wt01.temperature"),
                            "ln",
                            ORGANIZATION);
            client.close();
            logger.info("clear data of 127.0.0.1:8086 success!");

            client = InfluxDBClientFactory.create(URL2, TOKEN.toCharArray(), ORGANIZATION);

            client.getDeleteApi()
                    .delete(
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                            String.format(DELETE_DATA, "wf03", "wt01.status"),
                            "ln",
                            ORGANIZATION);

            client.getDeleteApi()
                    .delete(
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                            OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                            String.format(DELETE_DATA, "wf03", "wt01.temperature"),
                            "ln",
                            ORGANIZATION);
            client.close();
            logger.info("clear data of 127.0.0.1:8087 success!");
        } catch (Exception e) {
            logger.error("clear data fail! caused by {}", e.toString());
        }
    }
}
