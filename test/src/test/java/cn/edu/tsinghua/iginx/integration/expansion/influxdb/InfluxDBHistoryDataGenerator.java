package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBHistoryDataGenerator implements BaseHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBHistoryDataGenerator.class);

    public static final String TOKEN = "testToken";

    public static final String URL = "http://localhost:8086/";

    public static final String URL2 = "http://localhost:8087/";

    public static final String ORGANIZATION = "testOrg";

    private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\"";

    private static final WritePrecision WRITE_PRECISION = WritePrecision.MS;

    @Override
    public void writeHistoryDataToA() throws Exception {
        InfluxDBClient client = InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);

        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> ORGANIZATION.equals(o.getName()))
                .findFirst()
                .orElseThrow(IllegalAccessError::new);

        client.getBucketsApi().createBucket("ln", organization);
        List<Point> points = new ArrayList<>();

        long timestamp = 100;
        points.add(Point.measurement("wf01")
                .addField("wt01.status", true)
                .time(timestamp, WRITE_PRECISION));
        timestamp = 200;
        points.add(Point.measurement("wf01")
                .addField("wt01.status", false)
                .addField("wt01.temperature", 20.71)
                .time(timestamp, WRITE_PRECISION));

        client.getWriteApiBlocking().writePoints("data_center", organization.getId(), points);
        client.close();

        logger.info("write data to 127.0.0.1:8086 success!");
    }

    @Override
    public void writeHistoryDataToB() throws Exception {
        InfluxDBClient client = InfluxDBClientFactory.create(URL2, TOKEN.toCharArray(), ORGANIZATION);

        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> ORGANIZATION.equals(o.getName()))
                .findFirst()
                .orElseThrow(IllegalAccessError::new);

        client.getBucketsApi().createBucket("ln", organization);
        List<Point> points = new ArrayList<>();

        long timestamp = 77;
        points.add(Point.measurement("wf03")
                .addField("wt01.status", true)
                .time(timestamp, WRITE_PRECISION));
        timestamp = 200;
        points.add(Point.measurement("wf03")
                .addField("wt01.status", false)
                .addField("wt01.temperature", 77.71)
                .time(timestamp, WRITE_PRECISION));

        client.getWriteApiBlocking().writePoints("data_center", organization.getId(), points);
        client.close();

        logger.info("write data to 127.0.0.1:8087 success!");
    }

    @Override
    public void clearData() {
        InfluxDBClient client = InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);

        client.getDeleteApi().delete(
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                String.format(DELETE_DATA, "wf01", "wt01.status"),
                "ln",
                ORGANIZATION
        );

        client.getDeleteApi().delete(
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                String.format(DELETE_DATA, "wf01", "wt01.temperature"),
                "ln",
                ORGANIZATION
        );
        client.close();
        logger.info("clear data of 127.0.0.1:8086 success!");

        client = InfluxDBClientFactory.create(URL2, TOKEN.toCharArray(), ORGANIZATION);

        client.getDeleteApi().delete(
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                String.format(DELETE_DATA, "wf03", "wt01.status"),
                "ln",
                ORGANIZATION
        );

        client.getDeleteApi().delete(
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")),
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(500), ZoneId.of("UTC")),
                String.format(DELETE_DATA, "wf03", "wt01.temperature"),
                "ln",
                ORGANIZATION
        );
        client.close();
        logger.info("clear data of 127.0.0.1:8087 success!");
    }



    @Test
    public void writeHistoryData() {
        InfluxDBClient client = InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);

        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> ORGANIZATION.equals(o.getName()))
                .findFirst()
                .orElseThrow(IllegalAccessError::new);

        client.getBucketsApi().createBucket("data_center", organization);

        List<Point> points = new ArrayList<>();

        long timestamp = 1000 * 1000;

        Map<String, String> tags = new HashMap<>();
        tags.put("host", "1");
        tags.put("rack", "A");
        tags.put("room", "ROOMA");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 66.3).addField("temperature", 56.4)
                .time(timestamp, WRITE_PRECISION));
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 67.1).addField("temperature", 56.2)
                .time(timestamp + 1000 * 300, WRITE_PRECISION));

        tags = new HashMap<>();
        tags.put("host", "2");
        tags.put("rack", "B");
        tags.put("room", "ROOMA");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 72.1).addField("temperature", 55.1)
                .time(timestamp, WRITE_PRECISION));

        tags = new HashMap<>();
        tags.put("host", "4");
        tags.put("rack", "B");
        tags.put("room", "ROOMB");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 22.1).addField("temperature", 99.8)
                .time(timestamp + 1000 * 300, WRITE_PRECISION));


        client.getWriteApiBlocking().writePoints("data_center", organization.getId(), points);
        client.close();
    }

}
