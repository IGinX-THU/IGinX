/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.*;
import javax.annotation.Nonnull;

class DataViewWrapper {
  private final DataView dataView;

  private final Map<String, DataType> schema = new HashMap<>();

  private final List<String> fullNames = new ArrayList<>();

  public DataViewWrapper(DataView dataView) {
    this.dataView = dataView;
    for (int i = 0; i < dataView.getPathNum(); i++) {
      String path = dataView.getPath(i);
      Map<String, String> tags = dataView.getTags(i);
      String name = TagKVUtils.toFullName(path, tags);
      fullNames.add(name);
      DataType dataType = dataView.getDataType(i);
      schema.put(name, dataType);
    }
  }

  public boolean isRowData() {
    if (dataView.isRowData() && !dataView.isColumnData()) return true;
    else if (!dataView.isRowData() && dataView.isColumnData()) return false;
    else throw new IllegalArgumentException("DataView is neither row data nor column data");
  }

  public Map<String, DataType> getSchema() {
    return Collections.unmodifiableMap(schema);
  }

  public static Map.Entry<String, Map<String, String>> parseFieldName(String fieldName) {
    Pair<String, Map<String, String>> pathWithTags = TagKVUtils.fromFullName(fieldName);
    return new AbstractMap.SimpleImmutableEntry<>(pathWithTags.getK(), pathWithTags.getV());
  }

  public Scanner<Long, Scanner<String, Object>> getRowsScanner() {
    return new DataViewRowsScanner();
  }

  public Scanner<String, Scanner<Long, Object>> getColumnsScanner() {
    return new DataViewColumnsScanner();
  }

  private class DataViewRowsScanner implements Scanner<Long, Scanner<String, Object>> {

    private int keyIndex = -1;

    private Long key = null;

    Scanner<String, Object> rowScanner = null;

    public DataViewRowsScanner() {
      if (dataView.isColumnData()) {
        throw new IllegalArgumentException("DataView is column data");
      }
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
    public boolean iterate() throws StorageException {
      keyIndex++;
      if (keyIndex >= dataView.getKeySize()) {
        key = null;
        rowScanner = null;
        return false;
      }
      key = dataView.getKey(keyIndex);
      rowScanner = new RowScanner(keyIndex);

      return true;
    }

    @Override
    public void close() throws StorageException {}

    private class RowScanner implements Scanner<String, Object> {

      private final int keyIndex;

      private int fieldIndex = -1;

      private int valueIndex = -1;

      private String fieldName = null;

      private Object value = null;

      public RowScanner(int keyIndex) {
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
        } while (!dataView.getBitmapView(keyIndex).get(fieldIndex));
        valueIndex++;

        fieldName = fullNames.get(fieldIndex);
        value = dataView.getValue(keyIndex, valueIndex);
        return true;
      }

      @Override
      public void close() {}
    }
  }

  private class DataViewColumnsScanner implements Scanner<String, Scanner<Long, Object>> {

    private int fieldIndex = -1;

    private String fieldName = null;

    Scanner<Long, Object> columnScanner = null;

    public DataViewColumnsScanner() {
      if (dataView.isRowData()) {
        throw new IllegalArgumentException("DataView is row data");
      }
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
    public boolean iterate() throws StorageException {
      fieldIndex++;
      if (fieldIndex >= dataView.getPathNum()) {
        fieldName = null;
        columnScanner = null;
        return false;
      }
      fieldName = fullNames.get(fieldIndex);
      columnScanner = new ColumnScanner(fieldIndex);
      return true;
    }

    @Override
    public void close() throws StorageException {}

    private class ColumnScanner implements Scanner<Long, Object> {
      private final int fieldIndex;

      private final BitmapView bitmapView;

      private int keyIndex = -1;

      private int valueIndex = -1;

      private Long key = null;

      private Object value = null;

      public ColumnScanner(int fieldIndex) {
        this.fieldIndex = fieldIndex;
        this.bitmapView = dataView.getBitmapView(fieldIndex);
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
}
