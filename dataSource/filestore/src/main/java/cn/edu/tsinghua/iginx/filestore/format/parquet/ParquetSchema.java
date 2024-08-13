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

package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.UnsupportedTypeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ParquetSchema {
  private final Integer keyIndex;
  private final List<Pair<String, DataType>> rawHeader;
  private final List<Field> header;

  public ParquetSchema(MessageType schema, Map<String, String> extraMetadata, List<String> prefix) throws UnsupportedTypeException {
    Pair<Integer, List<Pair<String, DataType>>> pair = parseRawHeader(schema, extraMetadata);
    this.keyIndex = pair.getK();
    this.rawHeader = pair.getV();

    List<String> fullPrefix;
    if (schema.getName().isEmpty()) {
      fullPrefix = prefix;
    } else {
      fullPrefix = new ArrayList<>(prefix);
      fullPrefix.add(schema.getName());
    }

    this.header = parseHeader(rawHeader, keyIndex, fullPrefix);
  }

  public Integer getKeyIndex() {
    return this.keyIndex;
  }

  public List<Pair<String, DataType>> getRawHeader() {
    return this.rawHeader;
  }

  public List<Field> getHeader() {
    return header;
  }

  private Pair<Integer, List<Pair<String, DataType>>> parseRawHeader(MessageType schema, Map<String, String> extraMetadata) throws UnsupportedTypeException {
    boolean isIginxData = Constants.PARQUET_OBJECT_MODEL_NAME_VALUE.equals(extraMetadata.get(Constants.PARQUET_OBJECT_MODEL_NAME_PROP));

    List<Pair<String, DataType>> fields = new ArrayList<>();
    Integer keyIndex = null;
    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        throw new UnsupportedTypeException("Parquet", type);
      }

      String typeName = type.getName();
      if (isIginxData && typeName.equals(Constants.KEY_FIELD_NAME)) {
        keyIndex = fields.size();
        continue;
      }

      DataType iType = IParquetReader.toIginxType(type.asPrimitiveType());
      Pair<String, DataType> pair = new Pair<>(typeName, iType);
      fields.add(pair);
    }
    return new Pair<>(keyIndex, Collections.unmodifiableList(fields));
  }


  private static List<Field> parseHeader(List<Pair<String, DataType>> rawHeader, Integer keyIndex, List<String> prefix) {
    List<Field> fields = new ArrayList<>();

    for (Pair<String, DataType> rawField : rawHeader) {
      String typeName = rawField.getK();
      DataType iType = rawField.getV();

      if (!prefix.isEmpty()) {
        typeName = String.join(".", prefix) + "." + typeName;
      }

      Field field;
      if (keyIndex != null) {
        Pair<String, Map<String, String>> pair = TagKVUtils.fromFullName(typeName);
        field = new Field(pair.getK(), iType, pair.getV());
      } else {
        field = new Field(typeName, iType, Collections.emptyMap());
      }

      fields.add(field);
    }

    return Collections.unmodifiableList(fields);
  }
}
