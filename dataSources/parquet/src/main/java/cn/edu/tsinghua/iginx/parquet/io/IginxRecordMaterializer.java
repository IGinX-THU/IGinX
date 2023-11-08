package cn.edu.tsinghua.iginx.parquet.io;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class IginxRecordMaterializer extends RecordMaterializer<IginxRecord> {
  private final IginxGroupConverter root;

  public IginxRecordMaterializer(MessageType schema) {
    this.root = new IginxGroupConverter(schema);
  }

  @Override
  public IginxRecord getCurrentRecord() {
    return root.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
