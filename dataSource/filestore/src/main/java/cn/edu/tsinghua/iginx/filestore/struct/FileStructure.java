package cn.edu.tsinghua.iginx.filestore.struct;

import com.typesafe.config.Config;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.nio.file.Path;

@Immutable
public interface FileStructure {
  String getName();

  FileManager newReader(Path path, Config config) throws IOException;

  boolean supportWrite();

  FileManager newWriter(Path path, Config config) throws IOException;
}
