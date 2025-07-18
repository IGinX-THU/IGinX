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
    testAddRemoveAndShowDummyColumns();
  }

  private void testAddRemoveAndShowDummyColumns() {
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
    testAddRemoveAndShowDummyColumns();
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
        new TempDummyDataSource(session, filesystem, getFileTreeDummyParams())) {
      testQueryRawChunks();
      testQueryParquets();
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
    return params;
  }

  private void testShowDummyColumns() {
    String statement = "SHOW COLUMNS a.*;";
    String expected =
        "Columns:\n"
            + "+-----------------------------------------------------+--------+\n"
            + "|                                                 Path|DataType|\n"
            + "+-----------------------------------------------------+--------+\n"
            + "|                                       a.Iris\\parquet|  BINARY|\n"
            + "|                          a.Iris\\parquet.petal.length|  DOUBLE|\n"
            + "|                           a.Iris\\parquet.petal.width|  DOUBLE|\n"
            + "|                          a.Iris\\parquet.sepal.length|  DOUBLE|\n"
            + "|                           a.Iris\\parquet.sepal.width|  DOUBLE|\n"
            + "|                               a.Iris\\parquet.variety|  BINARY|\n"
            + "|                                        a.b.c.d.1\\txt|  BINARY|\n"
            + "|                                            a.e.2\\txt|  BINARY|\n"
            + "|                                          a.f.g.3\\txt|  BINARY|\n"
            + "|                                  a.floatTest\\parquet|  BINARY|\n"
            + "|                       a.floatTest\\parquet.floatValue|   FLOAT|\n"
            + "|                              a.other.MT cars\\parquet|  BINARY|\n"
            + "|                           a.other.MT cars\\parquet.am| INTEGER|\n"
            + "|                         a.other.MT cars\\parquet.carb| INTEGER|\n"
            + "|                          a.other.MT cars\\parquet.cyl| INTEGER|\n"
            + "|                         a.other.MT cars\\parquet.disp|  DOUBLE|\n"
            + "|                         a.other.MT cars\\parquet.drat|  DOUBLE|\n"
            + "|                         a.other.MT cars\\parquet.gear| INTEGER|\n"
            + "|                           a.other.MT cars\\parquet.hp| INTEGER|\n"
            + "|                        a.other.MT cars\\parquet.model|  BINARY|\n"
            + "|                          a.other.MT cars\\parquet.mpg|  DOUBLE|\n"
            + "|                         a.other.MT cars\\parquet.qsec|  DOUBLE|\n"
            + "|                           a.other.MT cars\\parquet.vs| INTEGER|\n"
            + "|                           a.other.MT cars\\parquet.wt|  DOUBLE|\n"
            + "|                                a.other.price\\parquet|  BINARY|\n"
            + "|                a.other.price\\parquet.airconditioning|  BINARY|\n"
            + "|                           a.other.price\\parquet.area|    LONG|\n"
            + "|                       a.other.price\\parquet.basement|  BINARY|\n"
            + "|                      a.other.price\\parquet.bathrooms|    LONG|\n"
            + "|                       a.other.price\\parquet.bedrooms|    LONG|\n"
            + "|               a.other.price\\parquet.furnishingstatus|  BINARY|\n"
            + "|                      a.other.price\\parquet.guestroom|  BINARY|\n"
            + "|                a.other.price\\parquet.hotwaterheating|  BINARY|\n"
            + "|                       a.other.price\\parquet.mainroad|  BINARY|\n"
            + "|                        a.other.price\\parquet.parking|    LONG|\n"
            + "|                       a.other.price\\parquet.prefarea|  BINARY|\n"
            + "|                          a.other.price\\parquet.price|    LONG|\n"
            + "|                        a.other.price\\parquet.stories|    LONG|\n"
            + "|                                   a.userdata\\parquet|  BINARY|\n"
            + "|                      a.userdata\\parquet.address.city|  BINARY|\n"
            + "|      a.userdata\\parquet.address.coordinates.latitude|  DOUBLE|\n"
            + "|     a.userdata\\parquet.address.coordinates.longitude|  DOUBLE|\n"
            + "|                    a.userdata\\parquet.address.street|  BINARY|\n"
            + "|                              a.userdata\\parquet.name|  BINARY|\n"
            + "|     a.userdata\\parquet.order_history.*.items.*.price|  DOUBLE|\n"
            + "|a.userdata\\parquet.order_history.*.items.*.product_id|  BINARY|\n"
            + "|  a.userdata\\parquet.order_history.*.items.*.quantity| INTEGER|\n"
            + "|        a.userdata\\parquet.order_history.*.order_date|    LONG|\n"
            + "|          a.userdata\\parquet.order_history.*.order_id|  BINARY|\n"
            + "|             a.userdata\\parquet.order_history.*.total|  DOUBLE|\n"
            + "|                   a.userdata\\parquet.phone_numbers.*|  BINARY|\n"
            + "|                           a.userdata\\parquet.user_id|  BINARY|\n"
            + "+-----------------------------------------------------+--------+\n"
            + "Total line number = 52\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
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

    // flat type in parquet that contains complex type
    statement = "select user_id, name from `a.userdata\\parquet`;";
    expect =
        "ResultSets:\n"
            + "+---+--------------------------+-----------------------+\n"
            + "|key|a.userdata\\parquet.user_id|a.userdata\\parquet.name|\n"
            + "+---+--------------------------+-----------------------+\n"
            + "|  0|                      U001|             John Smith|\n"
            + "|  1|                      U002|               Jane Doe|\n"
            + "|  2|                      U003|         Robert Johnson|\n"
            + "|  3|                      U004|           Emily Wilson|\n"
            + "|  4|                      U005|           Michael Chen|\n"
            + "+---+--------------------------+-----------------------+\n"
            + "Total line number = 5\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // nested type
    statement = "select city, street, coordinates from `a.userdata\\parquet`.address;";
    expect =
        "ResultSets:\n"
            + "+---+-------------------------------+---------------------------------+\n"
            + "|key|a.userdata\\parquet.address.city|a.userdata\\parquet.address.street|\n"
            + "+---+-------------------------------+---------------------------------+\n"
            + "|  0|                       New York|                  123 Main Street|\n"
            + "|  1|                         Boston|                   456 Oak Avenue|\n"
            + "|  2|                        Chicago|                  789 Pine Street|\n"
            + "|  3|                  San Francisco|                   101 Cedar Road|\n"
            + "|  4|                        Seattle|                  246 Maple Drive|\n"
            + "+---+-------------------------------+---------------------------------+\n"
            + "Total line number = 5\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // nested type with wildcard
    statement = "select user_id, address.* from `a.userdata\\parquet`;";
    expect =
        "ResultSets:\n"
            + "+---+--------------------------+-------------------------------+-----------------------------------------------+------------------------------------------------+---------------------------------+\n"
            + "|key|a.userdata\\parquet.user_id|a.userdata\\parquet.address.city|a.userdata\\parquet.address.coordinates.latitude|a.userdata\\parquet.address.coordinates.longitude|a.userdata\\parquet.address.street|\n"
            + "+---+--------------------------+-------------------------------+-----------------------------------------------+------------------------------------------------+---------------------------------+\n"
            + "|  0|                      U001|                       New York|                                        40.7128|                                         -74.006|                  123 Main Street|\n"
            + "|  1|                      U002|                         Boston|                                        42.3601|                                        -71.0589|                   456 Oak Avenue|\n"
            + "|  2|                      U003|                        Chicago|                                        41.8781|                                        -87.6298|                  789 Pine Street|\n"
            + "|  3|                      U004|                  San Francisco|                                        37.7749|                                       -122.4194|                   101 Cedar Road|\n"
            + "|  4|                      U005|                        Seattle|                                        47.6062|                                       -122.3321|                  246 Maple Drive|\n"
            + "+---+--------------------------+-------------------------------+-----------------------------------------------+------------------------------------------------+---------------------------------+\n"
            + "Total line number = 5\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // repeated type
    statement = "select `1`, `2` from `a.userdata\\parquet`.phone_numbers;";
    expect =
        "ResultSets:\n"
            + "+---+----------------------------------+----------------------------------+\n"
            + "|key|a.userdata\\parquet.phone_numbers.1|a.userdata\\parquet.phone_numbers.2|\n"
            + "+---+----------------------------------+----------------------------------+\n"
            + "|  0|                   +1-555-987-6543|                              null|\n"
            + "|  2|                   +1-555-456-7890|                   +1-555-567-8901|\n"
            + "+---+----------------------------------+----------------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // repeated with wildcard
    statement = "select * from `a.userdata\\parquet`.phone_numbers;";
    expect =
        "ResultSets:\n"
            + "+---+----------------------------------+----------------------------------+----------------------------------+\n"
            + "|key|a.userdata\\parquet.phone_numbers.0|a.userdata\\parquet.phone_numbers.1|a.userdata\\parquet.phone_numbers.2|\n"
            + "+---+----------------------------------+----------------------------------+----------------------------------+\n"
            + "|  0|                   +1-555-123-4567|                   +1-555-987-6543|                              null|\n"
            + "|  1|                   +1-555-234-5678|                              null|                              null|\n"
            + "|  2|                   +1-555-345-6789|                   +1-555-456-7890|                   +1-555-567-8901|\n"
            + "|  4|                   +1-555-678-9012|                              null|                              null|\n"
            + "+---+----------------------------------+----------------------------------+----------------------------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // nested repeated
    statement =
        "select `2.order_date`, `1.items.0.product_id` from `a.userdata\\parquet`.order_history;";
    expect =
        "ResultSets:\n"
            + "+---+---------------------------------------------+-----------------------------------------------------+\n"
            + "|key|a.userdata\\parquet.order_history.2.order_date|a.userdata\\parquet.order_history.1.items.0.product_id|\n"
            + "+---+---------------------------------------------+-----------------------------------------------------+\n"
            + "|  0|                                         null|                                                 P300|\n"
            + "|  4|                                1695915600000|                                                 P100|\n"
            + "+---+---------------------------------------------+-----------------------------------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // nested repeated with wildcard
    statement = "select *.items.*.price from `a.userdata\\parquet`.order_history;";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+\n"
            + "|key|a.userdata\\parquet.order_history.0.items.0.price|a.userdata\\parquet.order_history.0.items.1.price|a.userdata\\parquet.order_history.0.items.2.price|a.userdata\\parquet.order_history.1.items.0.price|a.userdata\\parquet.order_history.1.items.1.price|a.userdata\\parquet.order_history.2.items.0.price|\n"
            + "+---+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+\n"
            + "|  0|                                           29.99|                                           49.99|                                            null|                                           19.99|                                           99.99|                                            null|\n"
            + "|  1|                                          199.99|                                            null|                                            null|                                            null|                                            null|                                            null|\n"
            + "|  2|                                           49.99|                                           99.99|                                           29.99|                                            null|                                            null|                                            null|\n"
            + "|  4|                                           19.99|                                            null|                                            null|                                           29.99|                                           49.99|                                          199.99|\n"
            + "+---+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+------------------------------------------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // test filter for complex types
    statement =
        "select user_id from `a.userdata\\parquet` where order_history.`0`.items.`0`.price > 30.0;";
    expect =
        "ResultSets:\n"
            + "+---+--------------------------+\n"
            + "|key|a.userdata\\parquet.user_id|\n"
            + "+---+--------------------------+\n"
            + "|  1|                      U002|\n"
            + "|  2|                      U003|\n"
            + "+---+--------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}
