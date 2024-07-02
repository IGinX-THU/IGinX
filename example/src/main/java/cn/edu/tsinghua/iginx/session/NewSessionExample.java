/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
