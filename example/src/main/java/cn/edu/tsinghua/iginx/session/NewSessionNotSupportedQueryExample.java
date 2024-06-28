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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import java.util.List;

/**
 * Created on 10/12/2021. Description: 暂时只是样例，并不能实际运行
 *
 * @author ziyuan
 */
public class NewSessionNotSupportedQueryExample {

  public static void main(String[] args) {

    IginXClient client = IginXClientFactory.create();
    QueryClient queryClient = client.getQueryClient();

    IginXTable table =
        queryClient.query( // 查询 a.a.a 序列最近一秒内的数据
            SimpleQuery.builder()
                .addMeasurement("a.a.a")
                .startKey(System.currentTimeMillis() - 1000L)
                .endKey(System.currentTimeMillis())
                .build());
    List<POJO> pojoList =
        queryClient.query(
            "select * from demo.pojo where time < now() and time > now() - 1000",
            POJO.class); // 查询最近一秒内的 pojo 对象
    client.close();
  }

  @Measurement(name = "demo.pojo")
  static class POJO {

    @Field(timestamp = true)
    long timestamp;

    @Field int a;

    @Field int b;

    POJO(long timestamp, int a, int b) {
      this.timestamp = timestamp;
      this.a = a;
      this.b = b;
    }
  }
}
