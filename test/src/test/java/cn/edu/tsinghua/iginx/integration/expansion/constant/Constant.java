package cn.edu.tsinghua.iginx.integration.expansion.constant;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public static List<DataType> ORI_DATA_TYPE_LIST = Arrays.asList(DataType.LONG, DataType.DOUBLE);

  public static List<DataType> EXP_DATA_TYPE_LIST = Arrays.asList(DataType.LONG, DataType.DOUBLE);

  public static List<DataType> READ_ONLY_DATA_TYPE_LIST =
      Arrays.asList(DataType.LONG, DataType.DOUBLE);

  // values
  public static List<List<Object>> ORI_VALUES_LIST =
      Arrays.asList(Arrays.asList(11111111L, 15123.27), Arrays.asList(22222222L, 20123.71));

  public static List<List<Object>> EXP_VALUES_LIST =
      Arrays.asList(Arrays.asList(33333333L, 66123.23), Arrays.asList(44444444L, 77123.71));

  public static List<List<Object>> EXP_VALUES_LIST1 =
      Arrays.asList(Collections.singletonList(33333333L), Collections.singletonList(44444444L));

  public static List<List<Object>> EXP_VALUES_LIST2 =
      Arrays.asList(Collections.singletonList(66123.23), Collections.singletonList(77123.71));

  public static List<List<Object>> READ_ONLY_VALUES_LIST =
      Arrays.asList(Arrays.asList(55555555L, 10012.01), Arrays.asList(66666666L, 99123.99));

  public static List<List<Object>> REPEAT_EXP_VALUES_LIST1 =
      Arrays.asList(Arrays.asList(33333333L, 33333333L), Arrays.asList(44444444L, 44444444L));

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
}
