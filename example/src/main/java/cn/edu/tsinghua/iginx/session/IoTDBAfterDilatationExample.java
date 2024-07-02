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
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBAfterDilatationExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAfterDilatationExample.class);
  private static final String COLUMN_C1_S1 = "root.sg1.c1.s1";
  private static final String COLUMN_D1_S2 = "root.sg1.d1.s2";
  private static final String COLUMN_E2_S1 = "root.sg1.e2.s1";
  private static final String COLUMN_E3_S1 = "root.sg1.e3.s1";
  private static final long beginTimestamp = 1000000L;
  private static final int insertTimes = 1000;
  private static final int recordPerInsert = 100;
  private static final List<String> paths = new ArrayList<>();
  private static Session session;

  static {
    paths.add(COLUMN_C1_S1);
    paths.add(COLUMN_D1_S2);
    paths.add(COLUMN_E2_S1);
    paths.add(COLUMN_E3_S1);
  }

  public static void main(String[] args) throws Exception {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    // 插入数据
    insertRecords();
    // 关闭 session
    session.closeSession();
  }

  private static void insertRecords() throws SessionException, InterruptedException {
    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      dataTypeList.add(DataType.LONG);
    }

    for (int i = 0; i < insertTimes; i++) {
      long[] timestamps = new long[recordPerInsert];
      Object[] valuesList = new Object[paths.size()];
      for (int j = 0; j < recordPerInsert; j++) {
        timestamps[j] = beginTimestamp + (long) i * recordPerInsert + j;
      }
      for (int k = 0; k < paths.size(); k++) {
        Object[] values = new Object[recordPerInsert];
        for (int j = 0; j < recordPerInsert; j++) {
          values[j] = (long) (i + j + k);
        }
        valuesList[k] = values;
      }
      session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
      Thread.sleep(1);
      if ((i + 1) % 10 == 0) {
        LOGGER.info("insert progress: {}/1000.", (i + 1));
      }
    }
  }
}
