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
package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.filesystem;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.filesystem.format.csv.CsvFormat;
import cn.edu.tsinghua.iginx.filesystem.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filesystem.service.FileSystemConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTree;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.TempDummyDataSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemCapacityExpansionIT.class);

  public FileSystemCapacityExpansionIT() {
    super(filesystem, getAddStorageParams(), new FileSystemHistoryDataGenerator());
  }

  private static String getAddStorageParams() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("dummy.struct", FileSystemConfig.DEFAULT_DATA_STRUCT);
    return getAddStorageParams(params);
  }

  public static String getAddStorageParams(Map<String, String> params) {
    StringJoiner joiner = new StringJoiner(",");
    for (Map.Entry<String, String> entry : params.entrySet()) {
      joiner.add(entry.getKey() + "=" + entry.getValue());
    }
    return joiner.toString();
  }

  // skip this test
  @Override
  protected void testInvalidEngineParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    LOGGER.info("filesystem skips test for wrong engine params.");
  }

  @Override
  protected void testDatabaseShutdown() {
    LOGGER.info("filesystem skips test for shutting down data sources.");
  }

  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}

  @Override
  protected void shutdownDatabase(int port) {}

  @Override
  protected void startDatabase(int port) {}

  @Override
  public void testShowColumns() {
    super.testShowColumns();

    // show dummy columns
    try (TempDummyDataSource ignoredFileTree =
            new TempDummyDataSource(session, 16667, filesystem, getLegacyFileSystemDummyParams());
        TempDummyDataSource ignoredLegacyFileSystem =
            new TempDummyDataSource(session, 16668, filesystem, getFileTreeDummyParams())) {
      testShowDummyColumns();
    } catch (SessionException e) {
      LOGGER.error("add or remove read only storage engine failed ", e);
      fail();
    }
  }

  @Test
  public void testDummy() {
    testQuerySpecialHistoryData();
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testQueryLegacyFileSystem();
    testQueryFileTree();
  }

  private void testQueryLegacyFileSystem() {
    try (TempDummyDataSource ignored =
        new TempDummyDataSource(session, filesystem, getLegacyFileSystemDummyParams())) {
      testQueryRawChunks();
    } catch (SessionException e) {
      LOGGER.error("add or remove read only storage engine failed ", e);
      fail();
    }
  }

  private void testQueryFileTree() {
    try (TempDummyDataSource ignored =
            new TempDummyDataSource(session, 16667, filesystem, getFileTreeDummyParams());
        TempDummyDataSource ignoredCsv =
            new TempDummyDataSource(session, 16668, filesystem, getFileTreeCsvDummyParams())) {
      testQueryRawChunks();
      testQueryParquets();
      testQueryCSV();
    } catch (SessionException e) {
      LOGGER.error("add or remove read only storage engine failed ", e);
      fail();
    }
  }

  private static @NotNull Map<String, String> getLegacyFileSystemDummyParams() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("dummy_dir", "test/test/a");
    params.put("iginx_port", "6888");
    params.put("chunk_size_in_bytes", "1048576");
    return params;
  }

  private static @NotNull Map<String, String> getFileTreeDummyParams() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("dummy_dir", "test/test/a");
    params.put("iginx_port", "6888");
    params.put("dummy.struct", FileTree.NAME);
    params.put("dummy.config.formats." + RawFormat.NAME + ".pageSize", "1048576");
    params.put("dummy.config.formats." + CsvFormat.NAME + ".inferSchema", "true");
    params.put("dummy.config.formats." + CsvFormat.NAME + ".parseTypeFromHeader", "false");
    return params;
  }

  private static @NotNull Map<String, String> getFileTreeCsvDummyParams() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("dummy_dir", "test/test/csv");
    params.put("iginx_port", "6888");
    params.put("dummy.struct", FileTree.NAME);
    params.put("dummy.config.formats." + CsvFormat.NAME + ".dateFormat", "yyyy/MM/dd");
    params.put("dummy.config.formats." + CsvFormat.NAME + ".allowDuplicateColumnNames", "true");
    return params;
  }

  private void testShowDummyColumns() {
    String statement = "SHOW COLUMNS a.*;";
    String expected =
        "Columns:\n"
            + "+--------------------------------------+--------+\n"
            + "|                                  Path|DataType|\n"
            + "+--------------------------------------+--------+\n"
            + "|                   a.floatTest\\parquet|  BINARY|\n"
            + "|                        a.lineitem\\tsv|  BINARY|\n"
            + "|                        a.Iris\\parquet|  BINARY|\n"
            + "|                             a.e.2\\txt|  BINARY|\n"
            + "|                         a.b.c.d.1\\txt|  BINARY|\n"
            + "|                           a.f.g.3\\txt|  BINARY|\n"
            + "|               a.other.MT cars\\parquet|  BINARY|\n"
            + "|                 a.other.price\\parquet|  BINARY|\n"
            + "|        a.floatTest\\parquet.floatValue|   FLOAT|\n"
            + "|           a.other.MT cars\\parquet.cyl| INTEGER|\n"
            + "|         a.other.MT cars\\parquet.model|  BINARY|\n"
            + "|          a.other.MT cars\\parquet.carb| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.qsec|  DOUBLE|\n"
            + "|           a.other.MT cars\\parquet.mpg|  DOUBLE|\n"
            + "|            a.other.MT cars\\parquet.am| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.wt|  DOUBLE|\n"
            + "|            a.other.MT cars\\parquet.vs| INTEGER|\n"
            + "|            a.other.MT cars\\parquet.hp| INTEGER|\n"
            + "|          a.other.MT cars\\parquet.drat|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.disp|  DOUBLE|\n"
            + "|          a.other.MT cars\\parquet.gear| INTEGER|\n"
            + "|         a.other.price\\parquet.stories|    LONG|\n"
            + "|       a.other.price\\parquet.bathrooms|    LONG|\n"
            + "|        a.other.price\\parquet.mainroad|  BINARY|\n"
            + "|a.other.price\\parquet.furnishingstatus|  BINARY|\n"
            + "|       a.other.price\\parquet.guestroom|  BINARY|\n"
            + "|         a.other.price\\parquet.parking|    LONG|\n"
            + "|           a.other.price\\parquet.price|    LONG|\n"
            + "|        a.other.price\\parquet.bedrooms|    LONG|\n"
            + "|            a.other.price\\parquet.area|    LONG|\n"
            + "|        a.other.price\\parquet.basement|  BINARY|\n"
            + "| a.other.price\\parquet.airconditioning|  BINARY|\n"
            + "|        a.other.price\\parquet.prefarea|  BINARY|\n"
            + "| a.other.price\\parquet.hotwaterheating|  BINARY|\n"
            + "|              a.lineitem\\tsv.l_partkey|    LONG|\n"
            + "|                  a.lineitem\\tsv.l_tax|  DOUBLE|\n"
            + "|             a.lineitem\\tsv.l_shipmode|  BINARY|\n"
            + "|           a.lineitem\\tsv.l_linenumber|    LONG|\n"
            + "|           a.lineitem\\tsv.l_returnflag|  BINARY|\n"
            + "|           a.lineitem\\tsv.l_linestatus|  BINARY|\n"
            + "|        a.lineitem\\tsv.l_extendedprice|  DOUBLE|\n"
            + "|             a.lineitem\\tsv.l_shipdate|    LONG|\n"
            + "|             a.lineitem\\tsv.l_orderkey|    LONG|\n"
            + "|              a.lineitem\\tsv.l_comment|  BINARY|\n"
            + "|             a.lineitem\\tsv.l_discount|  DOUBLE|\n"
            + "|           a.lineitem\\tsv.l_commitdate|    LONG|\n"
            + "|         a.lineitem\\tsv.l_shipinstruct|  BINARY|\n"
            + "|              a.lineitem\\tsv.l_suppkey|    LONG|\n"
            + "|          a.lineitem\\tsv.l_receiptdate|    LONG|\n"
            + "|             a.lineitem\\tsv.l_quantity|    LONG|\n"
            + "|            a.Iris\\parquet.petal.width|  DOUBLE|\n"
            + "|            a.Iris\\parquet.sepal.width|  DOUBLE|\n"
            + "|           a.Iris\\parquet.sepal.length|  DOUBLE|\n"
            + "|           a.Iris\\parquet.petal.length|  DOUBLE|\n"
            + "|                a.Iris\\parquet.variety|  BINARY|\n"
            + "+--------------------------------------+--------+\n"
            + "Total line number = 55\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);
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

    // test float value compare
    statement = "select floatValue from `a.floatTest\\parquet` where floatValue >= 22.33;";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------+\n"
            + "|key|a.floatTest\\parquet.floatValue|\n"
            + "+---+------------------------------+\n"
            + "|  0|                         22.33|\n"
            + "|  1|                         44.55|\n"
            + "+---+------------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select floatValue from `a.floatTest\\parquet` where floatValue = 44.55;";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------+\n"
            + "|key|a.floatTest\\parquet.floatValue|\n"
            + "+---+------------------------------+\n"
            + "|  1|                         44.55|\n"
            + "+---+------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testQueryCSV() {
    String statement =
        "select l_extendedprice, l_shipdate from `a.lineitem\\tsv` where key >= 10 and key <20;";
    String expect =
        "ResultSets:\n"
            + "+---+------------------------------+-------------------------+\n"
            + "|key|a.lineitem\\tsv.l_extendedprice|a.lineitem\\tsv.l_shipdate|\n"
            + "+---+------------------------------+-------------------------+\n"
            + "| 10|                       1860.06|             754934400000|\n"
            + "| 11|                      30357.04|             755798400000|\n"
            + "| 12|                      25039.56|             751824000000|\n"
            + "| 13|                       29672.4|             821203200000|\n"
            + "| 14|                       15136.5|             783532800000|\n"
            + "| 15|                      26627.12|             782236800000|\n"
            + "| 16|                       46901.5|             776275200000|\n"
            + "| 17|                      38485.18|             704304000000|\n"
            + "| 18|                      12998.16|             831398400000|\n"
            + "| 19|                       9415.26|             823104000000|\n"
            + "+---+------------------------------+-------------------------+\n"
            + "Total line number = 10\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement =
        "select l_orderkey_1, l_orderkey_2, l_orderkey_3, l_shipdate from `csv.lineitem\\csv`;";
    expect =
        "ResultSets:\n"
            + "+---+-----------------------------+-----------------------------+-----------------------------+---------------------------+\n"
            + "|key|csv.lineitem\\csv.l_orderkey_1|csv.lineitem\\csv.l_orderkey_2|csv.lineitem\\csv.l_orderkey_3|csv.lineitem\\csv.l_shipdate|\n"
            + "+---+-----------------------------+-----------------------------+-----------------------------+---------------------------+\n"
            + "|  0|                            1|                            2|                            3|               826646400000|\n"
            + "|  1|                            1|                            2|                            3|               829238400000|\n"
            + "|  2|                            1|                            2|                            3|               822844800000|\n"
            + "|  3|                            1|                            2|                            3|               830016000000|\n"
            + "|  4|                            1|                            2|                            3|               828115200000|\n"
            + "+---+-----------------------------+-----------------------------+-----------------------------+---------------------------+\n"
            + "Total line number = 5\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}
