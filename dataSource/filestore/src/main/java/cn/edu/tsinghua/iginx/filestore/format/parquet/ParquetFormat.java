package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

@AutoService(FileFormat.class)
public class ParquetFormat implements FileFormat {

  public static final String NAME = "Parquet";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Collections.singletonList("parquet");
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    IParquetReader.Builder builder = IParquetReader.builder(path);
    ParquetMetadata footer;
    try (IParquetReader reader = builder.build()) {
      footer = reader.getMeta();
    }

    return new ParquetFormatReader(builder, footer, prefix);
  }
}
