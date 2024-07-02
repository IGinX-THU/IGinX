package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import java.util.Map;

public abstract class DataView {

  protected final RawData data;

  protected final int startPathIndex;

  protected final int endPathIndex;

  protected final int startKeyIndex;

  protected final int endKeyIndex;

  public DataView(
      RawData data, int startPathIndex, int endPathIndex, int startKeyIndex, int endKeyIndex) {
    this.data = data;
    this.startPathIndex = startPathIndex;
    this.endPathIndex = endPathIndex;
    this.startKeyIndex = startKeyIndex;
    this.endKeyIndex = endKeyIndex;
  }

  protected void checkPathIndexRange(int index) {
    if (index < 0 || index >= endPathIndex - startPathIndex)
      throw new IllegalArgumentException(
          String.format("path index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
  }

  protected void checkTypeIndexRange(int index) {
    if (index < 0 || index >= endPathIndex - startPathIndex)
      throw new IllegalArgumentException(
          String.format("type index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
  }

  protected void checkTimeIndexRange(int index) {
    if (index < 0 || index >= endKeyIndex - startKeyIndex)
      throw new IllegalArgumentException(
          String.format("time index out of range [%d, %d)", 0, endKeyIndex - startKeyIndex));
  }

  public int getPathNum() {
    return endPathIndex - startPathIndex;
  }

  public int getKeySize() {
    return endKeyIndex - startKeyIndex;
  }

  public boolean isRowData() {
    return data.isRowData();
  }

  public RawDataType getRawDataType() {
    return data.getType();
  }

  public boolean isColumnData() {
    return data.isColumnData();
  }

  public int getPathIndex(String path) {
    return data.getPaths().contains(path) ? data.getPaths().indexOf(path) - startPathIndex : -1;
  }

  public List<String> getPaths() {
    return data.getPaths().subList(startPathIndex, endPathIndex);
  }

  public String getPath(int index) {
    checkPathIndexRange(index);
    return data.getPaths().get(startPathIndex + index);
  }

  public List<DataType> getDataTypeList() {
    return data.getDataTypeList().subList(startPathIndex, endPathIndex);
  }

  public DataType getDataType(int index) {
    checkTypeIndexRange(index);
    return data.getDataTypeList().get(startPathIndex + index);
  }

  public int getKeyIndex(long timestamp) {
    return data.getKeys().contains(timestamp)
        ? data.getKeys().indexOf(timestamp) - startKeyIndex
        : -1;
  }

  public Long getKey(int index) {
    checkTimeIndexRange(index);
    return data.getKeys().get(startKeyIndex + index);
  }

  public abstract Object getValue(int index1, int index2);

  public abstract BitmapView getBitmapView(int index);

  public List<Map<String, String>> getTagsList() {
    if (data.getTagsList() != null && !data.getTagsList().isEmpty()) {
      return data.getTagsList().subList(startPathIndex, endPathIndex);
    }
    return data.getTagsList();
  }

  public Map<String, String> getTags(int index) {
    checkPathIndexRange(index);
    List<Map<String, String>> tagsList = data.getTagsList();
    if (tagsList == null || tagsList.isEmpty()) {
      return null;
    }
    return tagsList.get(startPathIndex + index);
  }
}
