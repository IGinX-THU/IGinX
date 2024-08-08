package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import com.typesafe.config.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public class FileTree implements FileStructure {

  public static final String NAME = "FileTree";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    return null;
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    return null;
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
