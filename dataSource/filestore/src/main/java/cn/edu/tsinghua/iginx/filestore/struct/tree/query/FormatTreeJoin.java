package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;

import javax.annotation.Nullable;
import java.nio.file.Path;

public class FormatTreeJoin implements QuerierBuilderFactory {
  @Override
  public QuerierBuilder create(Path path, @Nullable String prefix, FileTreeConfig config) {
    return null;
  }
}
