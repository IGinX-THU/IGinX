package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(FileStructure.class)
public class FileTree implements FileStructure {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTree.class);

  public static final String NAME = "FileTree";

  @Override
  public String getName() {
    return NAME;
  }

  @Value
  private static class Shared implements Closeable {

    FileTreeConfig config;

    @Override
    public void close() throws IOException {}
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    LOGGER.debug("Create shared instance with config: {}", config);
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(config);
    return new Shared(fileTreeConfig);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    LOGGER.debug("Create reader with path: {}", path);
    return new FileTreeManager(path, ((Shared) shared).getConfig());
  }

  @Override
  public boolean supportWrite() {
    return false;
  }

  @Override
  public FileManager newWriter(Path path, Closeable shared) throws IOException {
    throw new UnsupportedOperationException();
  }
}
