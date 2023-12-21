package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class DataViewColumnsScanner implements Scanner<String, Scanner<Long, Object>> {

  private final DataView dataView;

  private int fieldIndex = -1;

  private String fieldName = null;

  Scanner<Long, Object> columnScanner = null;

  public DataViewColumnsScanner(@Nonnull DataView dataView) {
    if (dataView.isRowData()) {
      throw new IllegalArgumentException("DataView is row data");
    }
    this.dataView = dataView;
  }

  @Nonnull
  @Override
  public String key() throws NoSuchElementException {
    if (fieldName == null) {
      throw new NoSuchElementException();
    }
    return fieldName;
  }

  @Nonnull
  @Override
  public Scanner<Long, Object> value() throws NoSuchElementException {
    if (columnScanner == null) {
      throw new NoSuchElementException();
    }
    return columnScanner;
  }

  @Override
  public boolean iterate() throws NativeStorageException {
    fieldIndex++;
    if (fieldIndex >= dataView.getPathNum()) {
      fieldName = null;
      columnScanner = null;
      return false;
    }
    fieldName = dataView.getPath(fieldIndex);
    columnScanner = new ColumnScanner(dataView, fieldIndex);

    return true;
  }

  @Override
  public void close() throws NativeStorageException {}

  private static class ColumnScanner implements Scanner<Long, Object> {

    private final DataView dataView;

    private final BitmapView bitmapView;

    private final int fieldIndex;

    private int keyIndex = -1;

    private int valueIndex = -1;

    private Long key = null;

    private Object value = null;

    public ColumnScanner(@Nonnull DataView dataView, int fieldIndex) {
      this.dataView = dataView;
      this.bitmapView = dataView.getBitmapView(fieldIndex);
      this.fieldIndex = fieldIndex;
    }

    @Nonnull
    @Override
    public Long key() throws NoSuchElementException {
      if (key == null) {
        throw new NoSuchElementException();
      }
      return key;
    }

    @Nonnull
    @Override
    public Object value() throws NoSuchElementException {
      if (key == null) {
        throw new NoSuchElementException();
      }
      return value;
    }

    @Override
    public boolean iterate() {
      do {
        keyIndex++;
        if (keyIndex >= dataView.getKeySize()) {
          key = null;
          value = null;
          return false;
        }
      } while (!bitmapView.get(keyIndex));
      valueIndex++;

      key = dataView.getKey(keyIndex);
      value = dataView.getValue(fieldIndex, valueIndex);
      return true;
    }

    @Override
    public void close() {}
  }
}
