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

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;

public class IginXStreamSessionExample {

  public static void main(String[] args) throws Exception {
    Session session = new Session("127.0.0.1", 6888, "root", "root");

    session.openSession();

    QueryDataSet dataSet = session.executeQuery("select * from computing_center", 10);

    List<String> columns = dataSet.getColumnList();
    List<DataType> dataTypes = dataSet.getDataTypeList();

    for (String column : columns) {
      System.out.print(column + "\t");
    }
    System.out.println();

    while (dataSet.hasMore()) {
      Object[] row = dataSet.nextRow();
      for (Object o : row) {
        System.out.print(o);
        System.out.print("\t\t\t");
      }
      System.out.println();
    }
    dataSet.close();

    session.closeSession();
  }
}
