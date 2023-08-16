package cn.edu.tsinghua.iginx.integration.expansion.constant;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
      Arrays.asList("mn.wf03.wt01.status", "nt.wf03.wt01.temperature");

  public static final List<String> EXP_PATH_LIST1 =
      Collections.singletonList("mn.wf03.wt01.status");

  public static final List<String> EXP_PATH_LIST2 =
      Collections.singletonList("nt.wf03.wt01.temperature");

  public static final List<String> READ_ONLY_PATH_LIST =
      Arrays.asList("mn.wf05.wt01.status", "mn.wf05.wt01.temperature");

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

  public static void setDataTypeAndValuesForFileSystem() {
    oriDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    expDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    readOnlyDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);

    byte[] oriValue = generateRandomValue(1);
    byte[] expValue = generateRandomValue(2);
    byte[] readOnlyValue = generateRandomValue(3);
    oriValuesList =
        Arrays.asList(Collections.singletonList(oriValue), Collections.singletonList(oriValue));
    expValuesList =
        Arrays.asList(Collections.singletonList(expValue), Collections.singletonList(expValue));
    expValuesList1 = Collections.singletonList(Collections.singletonList(expValue));
    expValuesList2 = Collections.singletonList(Collections.singletonList(expValue));
    readOnlyValuesList =
        Arrays.asList(
            Collections.singletonList(readOnlyValue), Collections.singletonList(readOnlyValue));
  }

  private static byte[] generateRandomValue(int seed) {
    int N = 10;
    byte[] b = new byte[N];
    Random random = new Random(seed);
    random.nextBytes(b);
    return b;
  }
}
