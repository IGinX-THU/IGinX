package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class DataViewSchemaScanner implements Scanner<String, Column> {

  private final DataView dataView;

  private int index = 0;

  private Column column = null;

  private String name = null;

  public DataViewSchemaScanner(DataView dataView) {
    this.dataView = dataView;
  }

  @Nonnull
  @Override
  public String key() throws NoSuchElementException {
    if (column == null) {
      throw new NoSuchElementException();
    }
    if (name == null) {
      name = TagKVUtils.toFullName(column.getPath(), column.getTags());
    }
    return name;
  }

  @Nonnull
  @Override
  public Column value() throws NoSuchElementException {
    if (column == null) {
      throw new NoSuchElementException();
    }
    return column;
  }

  @Override
  public boolean iterate() throws NativeStorageException {
    if (index >= dataView.getPathNum()) {
      column = null;
      name = null;
      return false;
    }

    String path = dataView.getPath(index);
    DataType dataType = dataView.getDataType(index);
    Map<String, String> tags = dataView.getTags(index);
    column = new Column(path, dataType, tags);
    name = null;
    index++;
    return true;
  }

  @Override
  public void close() throws NativeStorageException {}
}
