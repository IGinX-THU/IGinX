package cn.edu.tsinghua.iginx.parquet.io.parquet;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class IRecordMaterializer extends RecordMaterializer<IRecord> {
  private final IGroupConverter root;

  private IRecord currentRecord = null;

  public IRecordMaterializer(MessageType schema) {
    this.root =
        new IGroupConverter(
            schema,
            record -> {
              currentRecord = record;
            });
  }

  @Override
  public void skipCurrentRecord() {}

  @Override
  public IRecord getCurrentRecord() {
    return currentRecord;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
