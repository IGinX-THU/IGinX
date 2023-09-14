package cn.edu.tsinghua.iginx.integration.expansion.constant;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Constant {

  // port
  public static int oriPort = 6667;

  public static int expPort = 6668;

  public static int readOnlyPort = 6669;

  // path
  public static final List<String> ORI_PATH_LIST =
      Arrays.asList("mn.wf01.wt01.status", "mn.wf01.wt01.temperature");

  public static final List<String> EXP_PATH_LIST =
      Arrays.asList("nt.wf03.wt01.status2", "nt.wf04.wt01.temperature");

  public static final List<String> EXP_PATH_LIST1 =
      Collections.singletonList("nt.wf03.wt01.status2");

  public static final List<String> EXP_PATH_LIST2 =
      Collections.singletonList("nt.wf04.wt01.temperature");

  public static final List<String> READ_ONLY_PATH_LIST =
      Arrays.asList("tm.wf05.wt01.status", "tm.wf05.wt01.temperature");

  // data type
  public static List<DataType> oriDataTypeList = Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  public static List<DataType> expDataTypeList = Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  public static List<DataType> readOnlyDataTypeList =
      Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  // values
  public static List<List<Object>> oriValuesList =
      Arrays.asList(Arrays.asList(true, 15.27), Arrays.asList(false, 20.71));

  public static List<List<Object>> expValuesList =
      Arrays.asList(Arrays.asList(true, 66.23), Arrays.asList(false, 77.71));

  public static List<List<Object>> expValuesList1 =
      Arrays.asList(Collections.singletonList(true), Collections.singletonList(false));

  public static List<List<Object>> expValuesList2 =
      Arrays.asList(Collections.singletonList(66.23), Collections.singletonList(77.71));

  public static List<List<Object>> readOnlyValuesList =
      Arrays.asList(Arrays.asList(false, 100.01), Arrays.asList(true, 99.99));

  public static List<List<Object>> repeatExpValuesList1 =
      Arrays.asList(Arrays.asList(true, true), Arrays.asList(false, false));

  // for file system
  public static final Map<Integer, String> PORT_TO_ROOT =
      new HashMap<Integer, String>() {
        {
          put(oriPort, "mn");
          put(expPort, "nt");
          put(readOnlyPort, "tm");
        }
      };

  // for parquet
  // <port, [dataDir, dataFilename]>
  private static final String oriDir = "mn";

  private static final String expDir = "nt";

  private static final String readOnlyDir = "tm";

  private static final String oriFilename = "oriData.parquet";

  private static final String expFilename = "expData.parquet";

  private static final String readOnlyFilename = "readOnlyData.parquet";

  public static final Map<Integer, List<String>> PARQUET_PARAMS =
      new HashMap<Integer, List<String>>() {
        {
          put(oriPort, Arrays.asList(oriDir, oriFilename));
          put(expPort, Arrays.asList(expDir, expFilename));
          put(readOnlyPort, Arrays.asList(readOnlyDir, readOnlyFilename));
        }
      };

  public static byte[] generateRandomValue(int seed) {
    int N = 10;
    byte[] b = new byte[N];
    Random random = new Random(seed);
    random.nextBytes(b);
    return b;
  }
}
