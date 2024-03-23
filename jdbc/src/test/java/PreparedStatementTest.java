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
