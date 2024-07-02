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
import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResultSetTest {

  private ResultSet resultSet;

  @Before
  public void before() {
    resultSet = TestUtils.buildMockResultSet();
  }

  @After
  public void after() {
    resultSet = null;
  }

  @Test
  public void testFindColumn() throws SQLException {
    Assert.assertEquals(1, resultSet.findColumn(GlobalConstant.KEY_NAME));
    Assert.assertEquals(2, resultSet.findColumn("test.result.set.boolean"));
    Assert.assertEquals(3, resultSet.findColumn("test.result.set.int"));
    Assert.assertEquals(4, resultSet.findColumn("test.result.set.long"));
    Assert.assertEquals(5, resultSet.findColumn("test.result.set.float"));
    Assert.assertEquals(6, resultSet.findColumn("test.result.set.double"));
    Assert.assertEquals(7, resultSet.findColumn("test.result.set.string"));
  }

  @Test
  public void testGetFirstLine() throws SQLException {
    if (resultSet.next()) {
      // get by index
      Assert.assertEquals(1, resultSet.getLong(1));
      Assert.assertTrue(resultSet.getBoolean(2));
      Assert.assertEquals(1, resultSet.getInt(3));
      Assert.assertEquals(100000L, resultSet.getLong(4));
      Assert.assertEquals(10.1f, resultSet.getFloat(5), 0.0001);
      Assert.assertEquals(100.5, resultSet.getDouble(6), 0.000000001);
      Assert.assertEquals("one", resultSet.getString(7));
      // get by label
      Assert.assertEquals(1, resultSet.getLong(GlobalConstant.KEY_NAME));
      Assert.assertTrue(resultSet.getBoolean("test.result.set.boolean"));
      Assert.assertEquals(1, resultSet.getInt("test.result.set.int"));
      Assert.assertEquals(100000L, resultSet.getLong("test.result.set.long"));
      Assert.assertEquals(10.1f, resultSet.getFloat("test.result.set.float"), 0.0001);
      Assert.assertEquals(100.5, resultSet.getDouble("test.result.set.double"), 0.000000001);
      Assert.assertEquals("one", resultSet.getString("test.result.set.string"));
      // different type getString
      Assert.assertEquals("1", resultSet.getString(1));
      Assert.assertEquals("true", resultSet.getString(2));
      Assert.assertEquals("1", resultSet.getString(3));
      Assert.assertEquals("100000", resultSet.getString(4));
      Assert.assertEquals("10.1", resultSet.getString(5));
      Assert.assertEquals("100.5", resultSet.getString(6));
      Assert.assertEquals("one", resultSet.getString(7));
    }
  }

  @Test
  public void testGetNull() throws SQLException {
    if (resultSet.first()) resultSet.next(); // test null value which start from second line.

    // null boolean value will return false.
    Assert.assertFalse(resultSet.getBoolean("test.result.set.boolean"));

    if (resultSet.next()) // null integer value will return 0.
    Assert.assertEquals(0, resultSet.getInt("test.result.set.int"));

    if (resultSet.next()) // null long value will return 0.
    Assert.assertEquals(0, resultSet.getLong("test.result.set.long"));

    if (resultSet.next()) // null float value will return 0.
    Assert.assertEquals(0, resultSet.getFloat("test.result.set.float"), 0.0001);

    if (resultSet.next()) // null double value will return 0.
    Assert.assertEquals(0, resultSet.getDouble("test.result.set.double"), 0.000000001);

    if (resultSet.next()) // null string value will return null.
    Assert.assertEquals(null, resultSet.getString("test.result.set.string"));
  }
}
