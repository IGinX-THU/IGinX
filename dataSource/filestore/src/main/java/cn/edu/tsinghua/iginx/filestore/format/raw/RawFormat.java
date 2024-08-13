package cn.edu.tsinghua.iginx.filestore.format.raw;

import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

@AutoService(FileFormat.class)
public class RawFormat implements FileFormat {

  public static final String NAME = "RawChunk";

  private static final RawFormat INSTANCE = new RawFormat();

  public static RawFormat getInstance() {
    return INSTANCE;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Collections.emptyList();
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    if (prefix == null) {
      prefix = "";
    }
    RawReaderConfig rawReaderConfig = RawReaderConfig.of(config);
    return new RawReader(prefix, path, rawReaderConfig);
  }
}
