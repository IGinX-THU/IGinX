package cn.edu.tsinghua.iginx.session;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesFromBufferAndBitmaps;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSubPlanResp;
import java.util.ArrayList;
import java.util.List;

public class SessionExecuteSubPlanResult {

  private final long[] keys;
  private final List<String> paths;
  private final List<List<Object>> values;
  private final List<DataType> dataTypeList;

  public SessionExecuteSubPlanResult(ExecuteSubPlanResp resp) {
    this.paths = resp.getPaths();
    this.dataTypeList = resp.getDataTypeList();

    if (resp.getKeys() != null) {
      this.keys = getLongArrayFromByteBuffer(resp.keys);
    } else {
      this.keys = null;
    }

    // parse values
    if (resp.getQueryDataSet() != null) {
      this.values =
          getValuesFromBufferAndBitmaps(
              resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    } else {
      this.values = new ArrayList<>();
    }
  }

  public long[] getKeys() {
    return keys;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public List<DataType> getDataTypeList() {
    return dataTypeList;
  }
}
