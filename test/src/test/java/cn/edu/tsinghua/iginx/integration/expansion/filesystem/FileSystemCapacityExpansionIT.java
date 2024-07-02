package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.filesystem;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemCapacityExpansionIT.class);

  public FileSystemCapacityExpansionIT() {
    super(filesystem, "chunk_size_in_bytes:8", new FileSystemHistoryDataGenerator());
  }

  // skip this test
  @Override
  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    LOGGER.info("filesystem skips test for wrong dummy engine params.");
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
}
