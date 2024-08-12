package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.nio.file.Path;

@Immutable
public interface QuerierBuilderFactory {
  QuerierBuilder create(Path path, @Nullable String prefix, FileTreeConfig config);
}
