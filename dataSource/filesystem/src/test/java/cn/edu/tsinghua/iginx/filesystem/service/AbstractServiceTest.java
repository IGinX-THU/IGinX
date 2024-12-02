/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.service;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.test.DataViewGenerator;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractServiceTest {

  protected abstract Service getService() throws Exception;

  protected abstract DataUnit getUnit();

  private static DataView generateData(int size) {
    // construct insert statement
    List<String> pathList =
        new ArrayList<String>() {
          {
            add("us.d1.s1");
            add("us.d1.s2");
            add("us.d1.s3");
            add("us.d1.s4");
          }
        };
    List<DataType> dataTypeList =
        new ArrayList<DataType>() {
          {
            add(DataType.LONG);
            add(DataType.LONG);
            add(DataType.BINARY);
            add(DataType.DOUBLE);
          }
        };

    List<Object[]> valuesList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      valuesList.add(
          new Object[] {
            (long) i,
            (long) i + 1,
            ("\"" + RandomStringUtils.randomAlphanumeric(10) + "\"").getBytes(),
            (i + 0.1d)
          });
    }

    return DataViewGenerator.genRowDataViewNoKey(0, pathList, null, dataTypeList, valuesList);
  }

  private Service service;

  @BeforeEach
  public void setUp() throws Exception {
    getService().insert(getUnit(), generateData(10));
  }

  @AfterEach
  public void tearDown() throws Exception {
    getService().delete(getUnit(), new DataTarget(null, null, null));
  }

  @Test
  public void testInsertAndClear() {}
}
