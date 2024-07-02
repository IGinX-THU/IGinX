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
import cn.edu.tsinghua.iginx.jdbc.IginXPreparedStatement;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;

public class PreparedStatementTest {

  @Test
  public void testSetParams() throws SQLException {
    String preSQL =
        "SELECT a, b, c, d FROM root.sg WHERE TIME > ? AND TIME < ? AND a > ? OR b < ? AND c = ? AND d = ?;";
    IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
    ps.setLong(1, 10);
    ps.setLong(2, 15);
    ps.setFloat(3, 66.8f);
    ps.setDouble(4, 99.9);
    ps.setString(5, "abc");
    ps.setBoolean(6, true);

    String expectedSQL =
        "SELECT a, b, c, d FROM root.sg WHERE TIME > 10 AND TIME < 15 AND a > 66.8 OR b < 99.9 AND c = abc AND d = true;";
    String completeSQL = ps.getCompleteSql();
    Assert.assertEquals(expectedSQL, completeSQL);
    ps.close();
  }

  @Test
  public void testSetParamsWithSkipDoubleQuotes() throws SQLException {
    String preSQL =
        "SELECT a, b FROM root.sg WHERE TIME > 10 AND TIME < 25 AND a > ? AND b = \"asda?asd\";";
    IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
    ps.setLong(1, 10);

    String expectedSQL =
        "SELECT a, b FROM root.sg WHERE TIME > 10 AND TIME < 25 AND a > 10 AND b = \"asda?asd\";";
    String completeSQL = ps.getCompleteSql();
    Assert.assertEquals(expectedSQL, completeSQL);
    ps.close();
  }

  @Test
  public void testSetParamsWithSkipSingleQuote() throws SQLException {
    String preSQL =
        "SELECT a, b FROM root.sg WHERE TIME > 10 AND < 25 AND a > ? AND b = \'asda?asd\';";
    IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
    ps.setLong(1, 10);

    String expectedSQL =
        "SELECT a, b FROM root.sg WHERE TIME > 10 AND < 25 AND a > 10 AND b = \'asda?asd\';";
    String completeSQL = ps.getCompleteSql();
    Assert.assertEquals(expectedSQL, completeSQL);
    ps.close();
  }
}
