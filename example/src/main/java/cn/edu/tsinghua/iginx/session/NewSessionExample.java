package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import java.util.Arrays;

public class NewSessionExample {

  public static void main(String[] args) {
    IginXClient client = IginXClientFactory.create();
    WriteClient writeClient = client.getWriteClient();
    writeClient.writePoint(Point.builder().now().measurement("a.a.a").intValue(2333).build());
    writeClient.writePoint(
        Point.builder()
            .key(System.currentTimeMillis() - 1000L)
            .measurement("a.b.b")
            .doubleValue(2333.2)
            .build());

    long timestamp = System.currentTimeMillis();
    Point point1 = Point.builder().key(timestamp).measurement("a.a.a").intValue(666).build();
    Point point2 = Point.builder().key(timestamp).measurement("a.b.b").doubleValue(666.0).build();
    writeClient.writePoints(Arrays.asList(point1, point2));

    QueryClient queryClient = client.getQueryClient();
    IginXTable table =
        queryClient.query(
            SimpleQuery.builder()
                .addMeasurement("a.a.a")
                .addMeasurement("a.b.b")
                .endKey(System.currentTimeMillis() + 1000L)
                .build());
    System.out.println("Header:" + table.getHeader());

    for (IginXRecord record : table.getRecords()) {
      System.out.println(record);
    }

    client.close();
  }
}
