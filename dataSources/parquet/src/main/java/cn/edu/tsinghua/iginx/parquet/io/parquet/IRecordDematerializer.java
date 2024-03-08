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

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import java.util.Collections;
import java.util.Map;
import shaded.iginx.org.apache.parquet.io.api.Binary;
import shaded.iginx.org.apache.parquet.io.api.RecordConsumer;
import shaded.iginx.org.apache.parquet.io.api.RecordDematerializer;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

class IRecordDematerializer extends RecordDematerializer<IRecord> {

  private final MessageType schema;

  IRecordDematerializer(MessageType schema) {
    this.schema = schema;
  }

  private RecordConsumer recordConsumer = null;

  @Override
  public void setRecordConsumer(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(IRecord record) {
    if (recordConsumer != null) {
      recordConsumer.startMessage();
      addGroup(schema, record);
      recordConsumer.endMessage();
    }
  }

  @Override
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public Map<String, String> getExtraMetaData() {
    return Collections.emptyMap();
  }

  private void addGroup(GroupType groupType, IRecord record) {
    for (Map.Entry<Integer, Object> e : record) {
      int fieldIndex = e.getKey();
      Object fieldValue = e.getValue();
      Type fieldType = groupType.getType(fieldIndex);
      recordConsumer.startField(fieldType.getName(), fieldIndex);
      if (fieldType.isPrimitive()) {
        addPrimitive(fieldType.asPrimitiveType(), fieldValue);
      } else {
        recordConsumer.startGroup();
        addGroup(fieldType.asGroupType(), (IRecord) fieldValue);
        recordConsumer.endGroup();
      }
      recordConsumer.endField(fieldType.getName(), fieldIndex);
    }
  }

  private void addPrimitive(PrimitiveType fieldType, Object fieldValue) {
    PrimitiveType.PrimitiveTypeName typeName = fieldType.getPrimitiveTypeName();
    if (fieldType.getRepetition().equals(PrimitiveType.Repetition.REPEATED)) {
      for (Object o : (Object[]) fieldValue) {
        addValue(typeName, o);
      }
    } else {
      addValue(typeName, fieldValue);
    }
  }

  private void addValue(PrimitiveType.PrimitiveTypeName typeName, Object fieldValue) {
    switch (typeName) {
      case INT64:
        recordConsumer.addLong((Long) fieldValue);
        break;
      case INT32:
        recordConsumer.addInteger((Integer) fieldValue);
        break;
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) fieldValue);
        break;
      case BINARY:
        recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) fieldValue));
        break;
      case FLOAT:
        recordConsumer.addFloat((Float) fieldValue);
        break;
      case DOUBLE:
        recordConsumer.addDouble((Double) fieldValue);
        break;
      default:
        throw new IllegalStateException("Unexpected primitive type: " + typeName);
    }
  }
}
