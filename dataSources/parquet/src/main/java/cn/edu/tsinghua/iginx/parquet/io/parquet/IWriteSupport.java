package cn.edu.tsinghua.iginx.parquet.io.parquet;

import java.util.Map;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordDematerializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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
