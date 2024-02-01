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

package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

@Deprecated
public class Loader {
  private final Path path;

  public Loader(Path path) {
    this.path = path;
  }

  public Path getPath() {
    return path;
  }

  public Range<Long> getRange() throws Exception {
    IParquetReader.Builder builder = IParquetReader.builder(path);
    try (IParquetReader reader = builder.build()) {
      return reader.getRange();
    }
  }

  public List<Field> getHeader() throws IOException {
    Table table = new Table();
    IParquetReader.Builder builder = IParquetReader.builder(path);
    try (IParquetReader reader = builder.build()) {
      MessageType schema = reader.getSchema();

      Integer keyIndex = getFieldIndex(schema, Storer.KEY_FIELD_NAME);
      Map<List<Integer>, Integer> indexMap = new HashMap<>();
      List<Integer> schameIndexList = new ArrayList<>();
      List<String> typeNameList = new ArrayList<>();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (keyIndex != null && keyIndex == i) {
          continue;
        }
        schameIndexList.add(i);
        putIndexMap(schema.getType(i), typeNameList, schameIndexList, table, indexMap);
        schameIndexList.clear();
      }
      return table.getHeader();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("unexpected reader error!", e);
    }
  }

  public void load(Table table) throws IOException {

    IParquetReader.Builder builder = IParquetReader.builder(path);
    try (IParquetReader reader = builder.build()) {
      MessageType schema = reader.getSchema();

      Integer keyIndex = getFieldIndex(schema, Storer.KEY_FIELD_NAME);
      Map<List<Integer>, Integer> indexMap = new HashMap<>();
      List<Integer> schameIndexList = new ArrayList<>();
      List<String> typeNameList = new ArrayList<>();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (keyIndex != null && keyIndex == i) {
          continue;
        }
        schameIndexList.add(i);
        putIndexMap(schema.getType(i), typeNameList, schameIndexList, table, indexMap);
        schameIndexList.clear();
      }

      IRecord record;
      long cnt = 0;
      while ((record = reader.read()) != null) {
        Long key = null;
        if (keyIndex != null) {
          for (Map.Entry<Integer, Object> entry : record) {
            Integer index = entry.getKey();
            Object value = entry.getValue();
            if (index.equals(keyIndex)) {
              key = (Long) value;
              break;
            }
          }
          assert key != null;
        } else {
          key = cnt++;
        }

        List<Integer> indexList = new ArrayList<>();
        for (Map.Entry<Integer, Object> entry : record) {
          Integer index = entry.getKey();
          if (index.equals(keyIndex)) {
            continue;
          }
          indexList.add(index);
          Object value = entry.getValue();
          putValue(value, indexList, table, indexMap, key);
          indexList.clear();
        }
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("unexpected reader error!", e);
    }
  }

  private static void putValue(
      Object value,
      List<Integer> indexList,
      Table table,
      Map<List<Integer>, Integer> indexMap,
      Long key) {
    if (value instanceof IRecord) {
      IRecord record = (IRecord) value;
      for (Map.Entry<Integer, Object> entry : record) {
        Integer index = entry.getKey();
        Object v = entry.getValue();
        indexList.add(index);
        putValue(v, indexList, table, indexMap, key);
        indexList.remove(indexList.size() - 1);
      }
    } else {
      table.put(indexMap.get(indexList), key, value);
    }
  }

  private static void putIndexMap(
      Type type,
      List<String> typeNameList,
      List<Integer> indexList,
      Table table,
      Map<List<Integer>, Integer> indexMap) {
    typeNameList.add(type.getName());
    if (type.isPrimitive()) {
      PrimitiveType primitiveType = type.asPrimitiveType();
      DataType iginxType = ParquetMeta.toIginxType(primitiveType);
      String name = String.join(".", typeNameList);
      indexMap.put(new ArrayList<>(indexList), table.declareColumn(name, iginxType));
    } else {
      GroupType groupType = type.asGroupType();
      for (int i = 0; i < groupType.getFieldCount(); i++) {
        indexList.add(i);
        putIndexMap(groupType.getType(i), typeNameList, indexList, table, indexMap);
        indexList.remove(indexList.size() - 1);
      }
    }
    typeNameList.remove(typeNameList.size() - 1);
  }

  public static Integer getFieldIndex(MessageType schema, String fieldName) {
    try {
      return schema.getFieldIndex(fieldName);
    } catch (InvalidRecordException e) {
      return null;
    }
  }
}
