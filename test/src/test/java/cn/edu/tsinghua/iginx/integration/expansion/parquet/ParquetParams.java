package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ParquetParams {

  // <port, [dataDir, dataFilename]>
  private final HashMap<Integer, List<String>> params;

  private static final int oriPort = 6667;

  private static final int expPort = 6668;

  private static final int readOnlyPort = 6669;

  private static final String oriDir = "test/mn";

  private static final String expDir = "test/nt";

  private static final String readOnlyDir = "test/tm";

  private static final String oriFilename = "oriData.parquet";

  private static final String expFilename = "expData.parquet";

  private static final String readOnlyFilename = "readOnlyData.parquet";

  public ParquetParams() {
    this.params = new HashMap<>();
    params.put(oriPort, Arrays.asList(oriDir, oriFilename));
    params.put(expPort, Arrays.asList(expDir, expFilename));
    params.put(readOnlyPort, Arrays.asList(readOnlyDir, readOnlyFilename));
  }

  public HashMap<Integer, List<String>> getParams() {
    return params;
  }
}
