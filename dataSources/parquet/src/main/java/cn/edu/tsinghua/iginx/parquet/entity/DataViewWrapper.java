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

package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class DataViewWrapper {
    private final DataView dataView;

    private List<Column> header;

    public DataViewWrapper(DataView dataView) {
        this.dataView = dataView;
        for(int i = 0; i < dataView.getPathNum(); i++) {
            header.add(new Column(dataView.getPath(i), dataView.getDataType(i),dataView.getTags(i)));
        }
    }

    public List<Column> getHeader(){
        return Collections.unmodifiableList(header);
    }

    public Scanner<Long, Scanner<String, Object>> getRowsScanner() {
        return new DataViewRowsScanner(dataView);
    }

    public Scanner<String, Scanner<Long, Object>> getColumnsScanner() {
        return new DataViewColumnsScanner(dataView);
    }

    private static class DataViewRowsScanner implements Scanner<Long, Scanner<String, Object>> {

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

    private static class DataViewColumnsScanner implements Scanner<String, Scanner<Long, Object>> {

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
}
