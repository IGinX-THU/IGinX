package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import lombok.Value;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

@AutoService(FileStructure.class)
public class FileTree implements FileStructure {

  public static final String NAME = "FileTree";

  @Override
  public String getName() {
    return NAME;
  }

  @Value
  private static class Shared implements Closeable {

    FileTreeConfig config;

    @Override
    public void close() throws IOException {
    }
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(config);
    return new Shared(fileTreeConfig);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
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
