package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filestore.test.DataValidator;
import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.nio.file.Path;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class ParquetTestUtils {
  public static void createFile(Path path, Table table) throws IOException {
    MoreFiles.createParentDirectories(path);
    MessageType schema = ProjectUtils.toMessageType(table.getHeader());
    IParquetWriter.Builder writerBuilder = IParquetWriter.builder(path, schema);
    try (IParquetWriter writer = writerBuilder.build()) {
      for (Row row : table.getRows()) {
        Row stringAsBinary = DataValidator.withStringAsBinary(row);
        IRecord record = ProjectUtils.toRecord(stringAsBinary);
        writer.write(record);
      }
    }
  }
}
