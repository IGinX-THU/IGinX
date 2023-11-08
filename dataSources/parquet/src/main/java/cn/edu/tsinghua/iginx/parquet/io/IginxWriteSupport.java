package cn.edu.tsinghua.iginx.parquet.io;

import java.util.Collections;
import java.util.Map;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

class IginxWriteSupport extends WriteSupport<IginxRecord> {

  private final MessageType schema;

  IginxWriteSupport(MessageType schema) {
    this.schema = schema;
  }

  @Override
  public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
    return new WriteContext(schema, Collections.emptyMap());
  }

  private RecordConsumer recordConsumer;

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(IginxRecord record) {
    recordConsumer.startMessage();
    for (Map.Entry<Integer, Object> e : record) {
      int fieldIndex = e.getKey();
      Object fieldValue = e.getValue();
      PrimitiveType fieldType = schema.getType(fieldIndex).asPrimitiveType();
      recordConsumer.startField(fieldType.getName(), fieldIndex);
      switch (fieldType.getPrimitiveTypeName()) {
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
          throw new IllegalStateException(
              "Unexpected primitive type: " + fieldType.getPrimitiveTypeName());
      }
      recordConsumer.endField(fieldType.getName(), fieldIndex);
    }
    recordConsumer.endMessage();
  }
}
