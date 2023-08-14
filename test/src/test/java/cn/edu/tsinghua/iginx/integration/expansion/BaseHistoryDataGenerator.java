package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

  public int oriPort;

  public static final List<String> ORI_PATH_LIST =
      Arrays.asList("mn.wf01.wt01.status", "mn.wf01.wt01.temperature");

  public static final List<DataType> ORI_DATA_TYPE_LIST =
      Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  public static final List<List<Object>> ORI_VALUES_LIST =
      Arrays.asList(Arrays.asList(true, 15.27), Arrays.asList(false, 20.71));

  public int expPort;

  public static final List<String> EXP_PATH_LIST =
      Arrays.asList("mn.wf03.wt01.status", "nt.wf03.wt01.temperature");

  public static final List<String> EXP_PATH_LIST1 = Arrays.asList("mn.wf03.wt01.status");

  public static final List<String> EXP_PATH_LIST2 = Arrays.asList("nt.wf03.wt01.temperature");

  public static final List<DataType> EXP_DATA_TYPE_LIST =
      Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  public static final List<List<Object>> EXP_VALUES_LIST =
      Arrays.asList(Arrays.asList(true, 66.23), Arrays.asList(false, 77.71));

  public static final List<List<Object>> EXP_VALUES_LIST1 =
      Arrays.asList(Arrays.asList(true), Arrays.asList(false));

  public static final List<List<Object>> EXP_VALUES_LIST2 =
      Arrays.asList(Arrays.asList(66.23), Arrays.asList(77.71));

  public int readOnlyPort;

  public static final List<String> READ_ONLY_PATH_LIST =
      Arrays.asList("mn.wf05.wt01.status", "mn.wf05.wt01.temperature");

  public static final List<DataType> READ_ONLY_DATA_TYPE_LIST =
      Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

  public static final List<List<Object>> READ_ONLY_VALUES_LIST =
      Arrays.asList(Arrays.asList(false, 100.01), Arrays.asList(true, 99.99));

  public BaseHistoryDataGenerator() {}

  @Test
  public void oriHasDataExpHasData() {
    writeHistoryDataToOri();
    writeHistoryDataToExp();
    writeHistoryDataToReadOnly();
  }

  @Test
  public void oriHasDataExpNoData() {
    writeHistoryDataToOri();
  }

  @Test
  public void oriNoDataExpHasData() {
    writeHistoryDataToExp();
  }

  @Test
  public void oriNoDataExpNoData() {}

  public void writeHistoryDataToOri() {
    writeHistoryData(oriPort, ORI_PATH_LIST, ORI_DATA_TYPE_LIST, ORI_VALUES_LIST);
  }

  public void writeHistoryDataToExp() {
    writeHistoryData(expPort, EXP_PATH_LIST, EXP_DATA_TYPE_LIST, EXP_VALUES_LIST);
  }

  public void writeHistoryDataToReadOnly() {
    writeHistoryData(
        readOnlyPort, READ_ONLY_PATH_LIST, READ_ONLY_DATA_TYPE_LIST, READ_ONLY_VALUES_LIST);
  }

  public abstract void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList);

  @Test
  public void clearHistoryData() {
    clearHistoryDataForGivenPort(oriPort);
    clearHistoryDataForGivenPort(expPort);
    clearHistoryDataForGivenPort(readOnlyPort);
  }

  public abstract void clearHistoryDataForGivenPort(int port);
}
