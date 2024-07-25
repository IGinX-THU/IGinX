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
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreCapacityExpansionIT.class);

  public FileStoreCapacityExpansionIT() {
    super(filestore, "chunk_size_in_bytes:8", new FileStoreHistoryDataGenerator());
  }

  // skip this test
  @Override
  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    LOGGER.info("filestore skips test for wrong dummy engine params.");
  }

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
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|           a.b.c.d.1\\txt|  BINARY|\n"
            + "|               a.e.2\\txt|  BINARY|\n"
            + "|             a.f.g.3\\txt|  BINARY|\n"
            + "|          ln.wf02.status| BOOLEAN|\n"
            + "|         ln.wf02.version|  BINARY|\n"
            + "|     mn.wf01.wt01.status|  BINARY|\n"
            + "|mn.wf01.wt01.temperature|  BINARY|\n"
            + "|    nt.wf03.wt01.status2|  BINARY|\n"
            + "|nt.wf04.wt01.temperature|  BINARY|\n"
            + "|     tm.wf05.wt01.status|  BINARY|\n"
            + "|tm.wf05.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 11\n";
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
            + "+-------------+--------+\n"
            + "|         Path|DataType|\n"
            + "+-------------+--------+\n"
            + "|a.b.c.d.1\\txt|  BINARY|\n"
            + "|    a.e.2\\txt|  BINARY|\n"
            + "|  a.f.g.3\\txt|  BINARY|\n"
            + "+-------------+--------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    try {
      session.executeSql(
          "ADD STORAGEENGINE (\"127.0.0.1\", 6670, \"filestore\", \"dummy_dir:test/test/a, has_data:true, is_read_only:true, iginx_port:6888, chunk_size_in_bytes:1048576\");");
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
    } catch (SessionException e) {
      LOGGER.error("test query for file system failed ", e);
      fail();
    }
  }
}
