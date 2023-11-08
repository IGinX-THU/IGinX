package cn.edu.tsinghua.iginx.parquet.io;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class IginxRecordMaterializer extends RecordMaterializer<IginxRecord> {
  private final IginxGroupConverter root;

  private IginxRecord currentRecord = null;

  public IginxRecordMaterializer(MessageType schema) {
    this.root =
        new IginxGroupConverter(
            schema,
            record -> {
              currentRecord = record;
            });
  }

  @Override
  public IginxRecord getCurrentRecord() {
    return currentRecord;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
