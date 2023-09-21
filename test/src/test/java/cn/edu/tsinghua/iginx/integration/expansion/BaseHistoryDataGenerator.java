package cn.edu.tsinghua.iginx.integration.expansion;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

  public BaseHistoryDataGenerator() {}

  @Test
  public void oriHasDataExpHasData() {
    writeHistoryDataToOri();
    writeHistoryDataToExp();
    writeHistoryDataToReadOnly();
    writeSpecialHistoryData();
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

  public void writeSpecialHistoryData() {}

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
