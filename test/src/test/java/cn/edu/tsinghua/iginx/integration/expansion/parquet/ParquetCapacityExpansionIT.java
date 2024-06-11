package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.parquet;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.session.Column;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
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
  protected void testShowAllColumnsInExpansion(boolean before) {
    if (before) {
      testShowColumns(
          Arrays.asList(
              new Column("ln.wf02.status", DataType.BOOLEAN),
              new Column("ln.wf02.version", DataType.BINARY),
              new Column("nt.wf03.wt01.status2", DataType.LONG),
              new Column("nt.wf04.wt01.temperature", DataType.DOUBLE)));
    } else {
      testShowColumns(
          Arrays.asList(
              new Column("ln.wf02.status", DataType.BOOLEAN),
              new Column("ln.wf02.version", DataType.BINARY),
              new Column("nt.wf03.wt01.status2", DataType.LONG),
              new Column("p1.nt.wf03.wt01.status2", DataType.LONG),
              new Column("nt.wf04.wt01.temperature", DataType.DOUBLE)));
    }
  }
}
