package cn.edu.tsinghua.iginx.parquet.io;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class IginxReadSupport extends ReadSupport<IginxRecord> {

  @Override
  public ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return new ReadContext(fileSchema);
  }

  @Override
  public RecordMaterializer<IginxRecord> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return new IginxRecordMaterializer(fileSchema);
  }
}
