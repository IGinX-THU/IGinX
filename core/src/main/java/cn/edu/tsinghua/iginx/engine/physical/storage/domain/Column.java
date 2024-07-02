package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.*;
import java.util.stream.Collectors;

public final class Column {

  private String path;

  private final Map<String, String> tags;

  private final DataType dataType;

  private String physicalPath = null;

  private boolean isDummy = false;

  public Column(String path, DataType dataType) {
    this(path, dataType, null);
  }

  public Column(String path, DataType dataType, Map<String, String> tags) {
    this.path = path;
    this.dataType = dataType;
    this.tags = tags;
  }

  public Column(String path, DataType dataType, Map<String, String> tags, boolean isDummy) {
    this(path, dataType, tags);
    this.isDummy = isDummy;
  }

  public static RowStream toRowStream(Collection<Column> timeseries) {
    Header header =
        new Header(
            Arrays.asList(new Field("path", DataType.BINARY), new Field("type", DataType.BINARY)));
    List<Row> rows =
        timeseries.stream()
            .map(
                e ->
                    new Row(
                        header,
                        new Object[] {
                          TagKVUtils.toFullName(e.path, e.tags).getBytes(),
                          e.dataType.toString().getBytes()
                        }))
            .collect(Collectors.toList());
    return new Table(header, rows);
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getPhysicalPath() {
    if (physicalPath == null) {
      physicalPath = TagKVUtils.toPhysicalPath(path, tags);
    }
    return physicalPath;
  }

  public DataType getDataType() {
    return dataType;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public boolean isDummy() {
    return isDummy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column that = (Column) o;
    return Objects.equals(path, that.path)
        && dataType == that.dataType
        && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, dataType, tags);
  }
}
