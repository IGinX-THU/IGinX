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

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;

public class TagKVSessionExample {

  private static final String S1 = "ln.wf02.s";
  private static final String S2 = "ln.wf02.v";
  private static final String S3 = "ln.wf03.s";
  private static final String S4 = "ln.wf03.v";

  private static final long COLUMN_START_TIMESTAMP = 1L;
  private static final long COLUMN_END_TIMESTAMP = 10L;

  private static Session session;

  public static void main(String[] args) throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    // 列式插入对齐数据
    insertColumnRecords();

    // 查询数据
    queryData();

    // 关闭 Session
    session.closeSession();
  }

  private static void insertColumnRecords() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP + 1);
    long[] timestamps = new long[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = i + COLUMN_START_TIMESTAMP;
    }

    Object[] valuesList = new Object[4];
    for (long i = 0; i < 4; i++) {
      Object[] values = new Object[size];
      for (long j = 0; j < size; j++) {
        if (i < 2) {
          values[(int) j] = i + j;
        } else {
          values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
        }
      }
      valuesList[(int) i] = values;
    }

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.LONG);
    }
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.BINARY);
    }

    List<Map<String, String>> tagsList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      Map<String, String> tags = new HashMap<>();
      tags.put("k", "v" + i);
      tagsList.add(tags);
    }

    System.out.println("insertColumnRecords...");
    session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
  }

  private static void queryData() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    long startKey = COLUMN_START_TIMESTAMP + 2;
    long endKey = COLUMN_END_TIMESTAMP - 2;

    SessionQueryDataSet dataSet = session.queryData(paths, startKey, endKey, null);
    dataSet.print();

    List<Map<String, List<String>>> tagsList =
        Stream.of(
                new HashMap<String, List<String>>() {
                  {
                    put("k", Collections.singletonList("v1"));
                  }
                },
                new HashMap<String, List<String>>() {
                  {
                    put("k", Arrays.asList("v3"));
                  }
                })
            .collect(Collectors.toList());

    dataSet = session.queryData(paths, startKey, endKey, tagsList);
    dataSet.print();
  }
}
