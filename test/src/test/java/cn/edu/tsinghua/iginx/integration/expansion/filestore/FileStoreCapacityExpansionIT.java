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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.integration.expansion.filestore;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.filestore;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTree;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreCapacityExpansionIT.class);

  public FileStoreCapacityExpansionIT() {
    super(filestore, "dummy.config.chunk_size_in_bytes:8", new FileStoreHistoryDataGenerator());
  }

  // skip this test
  @Override
  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    LOGGER.info("filestore skips test for wrong dummy engine params.");
  }

  // filesystem中，所有dummy数据都识别为BINARY
  @Override
  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|nt.wf03.wt01.status2|  BINARY|\n"
            + "+--------------------+--------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS;";
    if (before) {
      expected =
          "Columns:\n"
              + "+------------------------+--------+\n"
              + "|                    Path|DataType|\n"
              + "+------------------------+--------+\n"
              + "|          ln.wf02.status| BOOLEAN|\n"
              + "|         ln.wf02.version|  BINARY|\n"
              + "|    nt.wf03.wt01.status2|  BINARY|\n"
              + "|nt.wf04.wt01.temperature|  BINARY|\n"
              + "+------------------------+--------+\n"
              + "Total line number = 4\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+------------------------+--------+\n"
              + "|                    Path|DataType|\n"
              + "+------------------------+--------+\n"
              + "|          ln.wf02.status| BOOLEAN|\n"
              + "|         ln.wf02.version|  BINARY|\n"
              + "|    nt.wf03.wt01.status2|  BINARY|\n"
              + "|nt.wf04.wt01.temperature|  BINARY|\n"
              + "| p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "+------------------------+--------+\n"
              + "Total line number = 5\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    if (before) {
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 1\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS *.wf03.wt01.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------+--------+\n"
              + "|                Path|DataType|\n"
              + "+--------------------+--------+\n"
              + "|nt.wf03.wt01.status2|  BINARY|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|  BINARY|\n"
              + "|p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  // filesystem中，所有dummy数据都识别为BINARY
  @Override
  protected void testShowColumnsRemoveStorageEngine(boolean before) {
    String statement = "SHOW COLUMNS p1.*, p2.*, p3.*;";
    String expected;
    if (before) {
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "|    p2.nt.wf03.wt01.status2|  BINARY|\n"
              + "|    p3.nt.wf03.wt01.status2|  BINARY|\n"
              + "|p3.nt.wf04.wt01.temperature|  BINARY|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 4\n";
    } else { // 移除schemaPrefix为p2及p3，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "|p3.nt.wf04.wt01.temperature|  BINARY|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}

  @Override
  public void testShowColumns() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|  BINARY|\n"
            + "|mn.wf01.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS;";
    expected =
        "Columns:\n"
            + "+--------------------------------------+--------+\n"
            + "|                                  Path|DataType|\n"
            + "+--------------------------------------+--------+\n"
            + "|                        a.Iris\\parquet|  BINARY|\n"
            + "|           a.Iris\\parquet.petal.length|  DOUBLE|\n"
            + "|            a.Iris\\parquet.petal.width|  DOUBLE|\n"
            + "|           a.Iris\\parquet.sepal.length|  DOUBLE|\n"
            + "|            a.Iris\\parquet.sepal.width|  DOUBLE|\n"
            + "|                a.Iris\\parquet.variety|  BINARY|\n"
            + "|                         a.b.c.d.1\\txt|  BINARY|\n"
            + "|                             a.e.2\\txt|  BINARY|\n"
            + "|                           a.f.g.3\\txt|  BINARY|\n"
            + "|               a.other.MT cars\\parquet|  BINARY|\n"
            + "|            a.other.MT cars\\parquet.am| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.carb| INTEGER|\n"
            + "|           a.other.MT cars\\parquet.cyl| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.disp|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.drat|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.gear| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.hp| INTEGER|\n"
            + "|         a.other.MT cars\\parquet.model|  BINARY|\n"
            + "|           a.other.MT cars\\parquet.mpg|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.qsec|  DOUBLE|\n"
            + "|            a.other.MT cars\\parquet.vs| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.wt|  DOUBLE|\n"
            + "|                 a.other.price\\parquet|  BINARY|\n"
            + "| a.other.price\\parquet.airconditioning|  BINARY|\n"
            + "|            a.other.price\\parquet.area|    LONG|\n"
            + "|        a.other.price\\parquet.basement|  BINARY|\n"
            + "|       a.other.price\\parquet.bathrooms|    LONG|\n"
            + "|        a.other.price\\parquet.bedrooms|    LONG|\n"
            + "|a.other.price\\parquet.furnishingstatus|  BINARY|\n"
            + "|       a.other.price\\parquet.guestroom|  BINARY|\n"
            + "| a.other.price\\parquet.hotwaterheating|  BINARY|\n"
            + "|        a.other.price\\parquet.mainroad|  BINARY|\n"
            + "|         a.other.price\\parquet.parking|    LONG|\n"
            + "|        a.other.price\\parquet.prefarea|  BINARY|\n"
            + "|           a.other.price\\parquet.price|    LONG|\n"
            + "|         a.other.price\\parquet.stories|    LONG|\n"
            + "|                        ln.wf02.status| BOOLEAN|\n"
            + "|                       ln.wf02.version|  BINARY|\n"
            + "|                   mn.wf01.wt01.status|  BINARY|\n"
            + "|              mn.wf01.wt01.temperature|  BINARY|\n"
            + "|                  nt.wf03.wt01.status2|  BINARY|\n"
            + "|              nt.wf04.wt01.temperature|  BINARY|\n"
            + "|                   tm.wf05.wt01.status|  BINARY|\n"
            + "|              tm.wf05.wt01.temperature|  BINARY|\n"
            + "+--------------------------------------+--------+\n"
            + "Total line number = 44\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|    nt.wf03.wt01.status2|  BINARY|\n"
            + "|nt.wf04.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     tm.wf05.wt01.status|  BINARY|\n"
            + "|tm.wf05.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS a.*;";
    expected =
        "Columns:\n"
            + "+--------------------------------------+--------+\n"
            + "|                                  Path|DataType|\n"
            + "+--------------------------------------+--------+\n"
            + "|                        a.Iris\\parquet|  BINARY|\n"
            + "|           a.Iris\\parquet.petal.length|  DOUBLE|\n"
            + "|            a.Iris\\parquet.petal.width|  DOUBLE|\n"
            + "|           a.Iris\\parquet.sepal.length|  DOUBLE|\n"
            + "|            a.Iris\\parquet.sepal.width|  DOUBLE|\n"
            + "|                a.Iris\\parquet.variety|  BINARY|\n"
            + "|                         a.b.c.d.1\\txt|  BINARY|\n"
            + "|                             a.e.2\\txt|  BINARY|\n"
            + "|                           a.f.g.3\\txt|  BINARY|\n"
            + "|               a.other.MT cars\\parquet|  BINARY|\n"
            + "|            a.other.MT cars\\parquet.am| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.carb| INTEGER|\n"
            + "|           a.other.MT cars\\parquet.cyl| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.disp|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.drat|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.gear| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.hp| INTEGER|\n"
            + "|         a.other.MT cars\\parquet.model|  BINARY|\n"
            + "|           a.other.MT cars\\parquet.mpg|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.qsec|  DOUBLE|\n"
            + "|            a.other.MT cars\\parquet.vs| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.wt|  DOUBLE|\n"
            + "|                 a.other.price\\parquet|  BINARY|\n"
            + "| a.other.price\\parquet.airconditioning|  BINARY|\n"
            + "|            a.other.price\\parquet.area|    LONG|\n"
            + "|        a.other.price\\parquet.basement|  BINARY|\n"
            + "|       a.other.price\\parquet.bathrooms|    LONG|\n"
            + "|        a.other.price\\parquet.bedrooms|    LONG|\n"
            + "|a.other.price\\parquet.furnishingstatus|  BINARY|\n"
            + "|       a.other.price\\parquet.guestroom|  BINARY|\n"
            + "| a.other.price\\parquet.hotwaterheating|  BINARY|\n"
            + "|        a.other.price\\parquet.mainroad|  BINARY|\n"
            + "|         a.other.price\\parquet.parking|    LONG|\n"
            + "|        a.other.price\\parquet.prefarea|  BINARY|\n"
            + "|           a.other.price\\parquet.price|    LONG|\n"
            + "|         a.other.price\\parquet.stories|    LONG|\n"
            + "+--------------------------------------+--------+\n"
            + "Total line number = 36\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  public static String getAddStorageParams(Map<String, String> params) {
    StringJoiner joiner = new StringJoiner(",");
    for (Map.Entry<String, String> entry : params.entrySet()) {
      joiner.add(entry.getKey() + ":" + entry.getValue());
    }
    return joiner.toString();
  }

  @Test
  public void testDummy() {
    testQuerySpecialHistoryData();
  }

  private void testQueryRawChunks() {
    String statement = "select 1\\txt from a.*;";
    String expect =
        "ResultSets:\n"
            + "+---+---------------------------------------------------------------------------+\n"
            + "|key|                                                              a.b.c.d.1\\txt|\n"
            + "+---+---------------------------------------------------------------------------+\n"
            + "|  0|979899100101102103104105106107108109110111112113114115116117118119120121122|\n"
            + "+---+---------------------------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select 2\\txt from a.*;";
    expect =
        "ResultSets:\n"
            + "+---+----------------------------------------------------+\n"
            + "|key|                                           a.e.2\\txt|\n"
            + "+---+----------------------------------------------------+\n"
            + "|  0|6566676869707172737475767778798081828384858687888990|\n"
            + "+---+----------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select 3\\txt from a.*;";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------------------+\n"
            + "|key|                               a.f.g.3\\txt|\n"
            + "+---+------------------------------------------+\n"
            + "|  0|012345678910111213141516171819202122232425|\n"
            + "+---+------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryParquets() {
    String statement;
    String expect;

    statement = "select petal.length from `a.Iris\\parquet` where key >= 10 and key <20;";
    expect =
        "ResultSets:\n"
            + "+---+---------------------------+\n"
            + "|key|a.Iris\\parquet.petal.length|\n"
            + "+---+---------------------------+\n"
            + "| 10|                        1.5|\n"
            + "| 11|                        1.6|\n"
            + "| 12|                        1.4|\n"
            + "| 13|                        1.1|\n"
            + "| 14|                        1.2|\n"
            + "| 15|                        1.5|\n"
            + "| 16|                        1.3|\n"
            + "| 17|                        1.4|\n"
            + "| 18|                        1.7|\n"
            + "| 19|                        1.5|\n"
            + "+---+---------------------------+\n"
            + "Total line number = 10\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement =
        "select `Iris\\parquet`.petal.length, other.`MT cars\\parquet`.mpg from a where key >= 10 and key <20;";
    expect =
        "ResultSets:\n"
            + "+---+---------------------------+---------------------------+\n"
            + "|key|a.Iris\\parquet.petal.length|a.other.MT cars\\parquet.mpg|\n"
            + "+---+---------------------------+---------------------------+\n"
            + "| 10|                        1.5|                       17.8|\n"
            + "| 11|                        1.6|                       16.4|\n"
            + "| 12|                        1.4|                       17.3|\n"
            + "| 13|                        1.1|                       15.2|\n"
            + "| 14|                        1.2|                       10.4|\n"
            + "| 15|                        1.5|                       10.4|\n"
            + "| 16|                        1.3|                       14.7|\n"
            + "| 17|                        1.4|                       32.4|\n"
            + "| 18|                        1.7|                       30.4|\n"
            + "| 19|                        1.5|                       33.9|\n"
            + "+---+---------------------------+---------------------------+\n"
            + "Total line number = 10\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select disp, furnishingstatus from a.* where key >= 10 and key <20;";
    expect =
        "ResultSets:\n"
            + "+---+----------------------------+--------------------------------------+\n"
            + "|key|a.other.MT cars\\parquet.disp|a.other.price\\parquet.furnishingstatus|\n"
            + "+---+----------------------------+--------------------------------------+\n"
            + "| 10|                       167.6|                             furnished|\n"
            + "| 11|                       275.8|                        semi-furnished|\n"
            + "| 12|                       275.8|                        semi-furnished|\n"
            + "| 13|                       275.8|                             furnished|\n"
            + "| 14|                       472.0|                        semi-furnished|\n"
            + "| 15|                       460.0|                        semi-furnished|\n"
            + "| 16|                       440.0|                           unfurnished|\n"
            + "| 17|                        78.7|                             furnished|\n"
            + "| 18|                        75.7|                             furnished|\n"
            + "| 19|                        71.1|                        semi-furnished|\n"
            + "+---+----------------------------+--------------------------------------+\n"
            + "Total line number = 10\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement =
        "select Iris\\parquet.petal.length from a where key < 50 and other.price\\parquet.furnishingstatus ==\"unfurnished\";";
    expect =
        "ResultSets:\n"
            + "+---+---------------------------+\n"
            + "|key|a.Iris\\parquet.petal.length|\n"
            + "+---+---------------------------+\n"
            + "|  7|                        1.5|\n"
            + "|  9|                        1.5|\n"
            + "| 16|                        1.3|\n"
            + "| 21|                        1.5|\n"
            + "| 28|                        1.4|\n"
            + "| 30|                        1.6|\n"
            + "| 33|                        1.4|\n"
            + "| 38|                        1.3|\n"
            + "| 42|                        1.3|\n"
            + "| 48|                        1.5|\n"
            + "+---+---------------------------+\n"
            + "Total line number = 10\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryLegacyFileSystem() {
    try {
      session.executeSql(
          "ADD STORAGEENGINE (\"127.0.0.1\", 6670, \"filestore\", \"dummy_dir:test/test/a, has_data:true, is_read_only:true, iginx_port:6888, chunk_size_in_bytes:1048576\");");

    } catch (SessionException e) {
      LOGGER.error("test query for file system failed ", e);
      fail();
    }
    testQueryRawChunks();
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testQueryLegacyFileSystem();
    testQueryFileTree();
  }

  private void testQueryFileTree() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("dummy_dir", "test/test/a");
    params.put("has_data", "true");
    params.put("is_read_only", "true");
    params.put("iginx_port", "6888");
    // dummy.struct=LegacyFilesystem#dummy.config.
    params.put("dummy.struct", FileTree.NAME);
    params.put("dummy.config.formats." + RawFormat.NAME + ".pageSize", "1048576");
    String addStorageParams = getAddStorageParams(params);

    try {
      session.executeSql(
          "ADD STORAGEENGINE (\"127.0.0.1\", 6671, \"filestore\", \"" + addStorageParams + "\");");
    } catch (SessionException e) {
      LOGGER.error("add storage engine failed ", e);
      if (!e.getMessage().contains("repeatedly")) {
        fail();
      }
    }

    testQueryRawChunks();
    testQueryParquets();
  }
}
