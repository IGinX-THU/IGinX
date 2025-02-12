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
import cn.edu.tsinghua.iginx.jdbc.IginXStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class BatchTest {

  @Test
  public void testBatch() throws SQLException {
    List<String> sqlList =
        new ArrayList<>(
            Arrays.asList(
                "INSERT INTO test.batch (a, b, c) values (1, 1.1, \"one\");",
                "INSERT INTO test.batch (a, b, c) values (2, 2.1, \"two\");",
                "INSERT INTO test.batch (a, b, c) values (3, 3.1, \"three\");",
                "DELETE FROM test.batch.c WHERE c = \"two\"",
                "DELETE FROM test.batch.c WHERE c = \"three\"",
                "ADD STORAGEENGINE (\"127.0.0.1\", 6667, IOTDB, \"{\"hello\"=\"world\"}\");"));

    IginXStatement statement = new IginXStatement(null, null);

    for (String sql : sqlList) {
      statement.addBatch(sql);
    }
    Assert.assertEquals(sqlList, statement.getBatchSQLList());

    statement.clearBatch();
    Assert.assertEquals(Collections.emptyList(), statement.getBatchSQLList());
    statement.close();
  }

  /* Batch query is not supported. */

  @Test(expected = SQLException.class)
  public void testBatchWithSelect() throws SQLException {
    IginXStatement statement = new IginXStatement(null, null);
    String sql = "SELECT a FROM test.batch WHERE a < 10;";
    statement.addBatch(sql);
    statement.close();
  }

  @Test(expected = SQLException.class)
  public void testBatchWithShow() throws SQLException {
    IginXStatement statement = new IginXStatement(null, null);
    String sql = "SHOW REPLICATION;";
    statement.addBatch(sql);
    statement.close();
  }
}
