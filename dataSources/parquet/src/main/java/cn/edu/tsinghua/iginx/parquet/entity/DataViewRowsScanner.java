package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;

import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class DataViewRowsScanner implements Scanner<Long, Scanner<String, Object>> {

  private final DataView dataView;

  private int keyIndex = -1;

  private Long key = null;

  Scanner<String, Object> rowScanner = null;

  public DataViewRowsScanner(@Nonnull DataView dataView) {
    if (dataView.isColumnData()) {
      throw new IllegalArgumentException("DataView is column data");
    }
    this.dataView = dataView;
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
  public Scanner<String, Object> value() throws NoSuchElementException {
    if (rowScanner == null) {
      throw new NoSuchElementException();
    }
    return rowScanner;
  }

  @Override
  public boolean iterate() throws NativeStorageException {
    keyIndex++;
    if (keyIndex >= dataView.getKeySize()) {
      key = null;
      rowScanner = null;
      return false;
    }
    key = dataView.getKey(keyIndex);
    rowScanner = new RowScanner(dataView, keyIndex);

    return true;
  }

  @Override
  public void close() throws NativeStorageException {}

  private static class RowScanner implements Scanner<String, Object> {

    private final DataView dataView;

    private final BitmapView bitmapView;

    private final int keyIndex;

    private int fieldIndex = -1;

    private int valueIndex = -1;

    private String fieldName = null;

    private Object value = null;

    public RowScanner(@Nonnull DataView dataView, int keyIndex) {
      this.dataView = dataView;
      this.bitmapView = dataView.getBitmapView(keyIndex);
      this.keyIndex = keyIndex;
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
    public Object value() throws NoSuchElementException {
      if (fieldName == null) {
        throw new NoSuchElementException();
      }
      return value;
    }

    @Override
    public boolean iterate() {
      do {
        fieldIndex++;
        if (fieldIndex >= dataView.getPathNum()) {
          fieldName = null;
          value = null;
          return false;
        }
      } while (!bitmapView.get(fieldIndex));
      valueIndex++;

      fieldName = dataView.getPath(fieldIndex);
      value = dataView.getValue(keyIndex, valueIndex);
      return true;
    }

    @Override
    public void close() {}
  }
}
