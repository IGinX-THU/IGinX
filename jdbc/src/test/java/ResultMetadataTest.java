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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResultMetadataTest {

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
  public void testMetaDataColumnName() throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    Assert.assertEquals("key", metaData.getColumnName(1));
    Assert.assertEquals("test.result.set.boolean", metaData.getColumnName(2));
    Assert.assertEquals("test.result.set.int", metaData.getColumnName(3));
    Assert.assertEquals("test.result.set.long", metaData.getColumnName(4));
    Assert.assertEquals("test.result.set.float", metaData.getColumnName(5));
    Assert.assertEquals("test.result.set.double", metaData.getColumnName(6));
    Assert.assertEquals("test.result.set.string", metaData.getColumnName(7));
  }

  @Test
  public void testMetaDataColumnType() throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    Assert.assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
    Assert.assertEquals(Types.BOOLEAN, metaData.getColumnType(2));
    Assert.assertEquals(Types.INTEGER, metaData.getColumnType(3));
    Assert.assertEquals(Types.BIGINT, metaData.getColumnType(4));
    Assert.assertEquals(Types.FLOAT, metaData.getColumnType(5));
    Assert.assertEquals(Types.DOUBLE, metaData.getColumnType(6));
    Assert.assertEquals(Types.VARCHAR, metaData.getColumnType(7));
  }

  @Test
  public void testMetaDataColumnCount() throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    Assert.assertEquals(7, metaData.getColumnCount());
  }
}
