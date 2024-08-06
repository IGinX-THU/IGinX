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

package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.parquet;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetCapacityExpansionIT.class);

  public ParquetCapacityExpansionIT() {
    super(parquet, null, new ParquetHistoryDataGenerator());
  }

  // skip this test
  @Override
  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    LOGGER.info("parquet skips test for wrong dummy engine params.");
  }

  @Override
  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|nt.wf03.wt01.status2|    LONG|\n"
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
              + "|    nt.wf03.wt01.status2|    LONG|\n"
              + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
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
              + "|    nt.wf03.wt01.status2|    LONG|\n"
              + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "| p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+------------------------+--------+\n"
              + "Total line number = 5\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS p1.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
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
              + "|nt.wf03.wt01.status2|    LONG|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|    LONG|\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  // no param is allowed to be updated
  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}
}
