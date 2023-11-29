package cn.edu.tsinghua.iginx.parquet.io.parquet.impl;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class IReadSupport extends ReadSupport<IRecord> {

  @Override
  public ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return new ReadContext(fileSchema);
  }

  @Override
  public RecordMaterializer<IRecord> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return new IRecordMaterializer(fileSchema);
  }
}
