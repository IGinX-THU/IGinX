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
import java.util.Collections;

public class ParquetServerExample {

  private static Session session1;

  private static Session session2;

  /** session1 parquet use local session2 parquet use remote */
  public static void main(String[] args) throws SessionException {
    // init
    session1 = new Session("127.0.0.1", 6888, "root", "root");
    session1.openSession();
    session2 = new Session("127.0.0.1", 6889, "root", "root");
    session2.openSession();

    // local insert
    session1.executeSql(
        "INSERT INTO test(key, s1, s2, s3) VALUES "
            + "(1, 1, 1.5, \"test1\"), "
            + "(2, 2, 2.5, \"test2\"), "
            + "(3, 3, 3.5, \"test3\"), "
            + "(4, 4, 4.5, \"test4\"), "
            + "(5, 5, 5.5, \"test5\"); ");

    System.out.println("================================");

    // local query and get time series
    System.out.println("local result:");
    SessionExecuteSqlResult result1 = session1.executeSql("SELECT * FROM test");
    result1.print(false, "");

    result1 = session1.executeSql("SHOW COLUMNS");
    result1.print(false, "");

    System.out.println("================================");

    // remote query and get time series
    System.out.println("remote result:");
    SessionExecuteSqlResult result2 = session2.executeSql("SELECT * FROM test");
    result2.print(false, "");

    result2 = session2.executeSql("SHOW COLUMNS");
    result2.print(false, "");

    System.out.println("================================");

    // remote delete data and local query
    session2.executeSql("DELETE FROM test.s3 WHERE key > 3");

    result1 = session1.executeSql("SELECT * FROM test");
    result1.print(false, "");

    System.out.println("================================");

    // remote delete cols and local query
    session2.deleteColumns(Collections.singletonList("test.s3"));

    result1 = session1.executeSql("SELECT * FROM test");
    result1.print(false, "");

    result1 = session1.executeSql("SHOW COLUMNS");
    result1.print(false, "");

    System.out.println("================================");

    session2.executeSql(
        "INSERT INTO test(key, s4, s5) VALUES "
            + "(6, 6.1, \"test6\"), "
            + "(7, 7.1, \"test7\"), "
            + "(8, 8.1, \"test8\")");

    result1 = session1.executeSql("SELECT * FROM test");
    result1.print(false, "");

    result1 = session1.executeSql("SHOW COLUMNS");
    result1.print(false, "");

    session1.closeSession();
    session2.closeSession();
  }
}
