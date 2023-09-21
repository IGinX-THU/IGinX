package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(InfluxDBHistoryDataGenerator.class);

  public static final String TOKEN = "testToken";

  public static final String ORGANIZATION = "testOrg";

  private static final WritePrecision WRITE_PRECISION = WritePrecision.NS;

  public InfluxDBHistoryDataGenerator() {
    Constant.oriPort = 8086;
    Constant.expPort = 8087;
    Constant.readOnlyPort = 8088;
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    String url = "http://localhost:" + port + "/";
    InfluxDBClient client = InfluxDBClientFactory.create(url, TOKEN.toCharArray(), ORGANIZATION);
    Organization organization =
        client.getOrganizationsApi().findOrganizations().stream()
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
        if (valueList.get(i) == null) {
          continue;
        }
        switch (dataType) {
          case BOOLEAN:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1), (boolean) valueList.get(i))
                    .time(timeCnt, WRITE_PRECISION);
            break;
          case BINARY:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1),
                        new String((byte[]) valueList.get(i)))
                    .time(timeCnt, WRITE_PRECISION);
            break;
          case DOUBLE:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1),
                        (Number) (double) valueList.get(i))
                    .time(timeCnt, WRITE_PRECISION);
            break;
          case FLOAT:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1),
                        (Number) (float) valueList.get(i))
                    .time(timeCnt, WRITE_PRECISION);
            break;
          case LONG:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1),
                        (Number) (long) valueList.get(i))
                    .time(timeCnt, WRITE_PRECISION);
            break;
          case INTEGER:
            point =
                Point.measurement(measurementName)
                    .addField(
                        fieldName.substring(0, fieldName.length() - 1),
                        (Number) (int) valueList.get(i))
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
    logger.info("write data to " + url + " success!");
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    String url = "http://localhost:" + port + "/";
    InfluxDBClient client = InfluxDBClientFactory.create(url, TOKEN.toCharArray(), ORGANIZATION);
    Bucket bucket = client.getBucketsApi().findBucketByName("mn");
    if (bucket != null) {
      client.getBucketsApi().deleteBucket(bucket);
    }
    client.close();
    logger.info("clear data on 127.0.0.1:{} success!", port);
  }
}
