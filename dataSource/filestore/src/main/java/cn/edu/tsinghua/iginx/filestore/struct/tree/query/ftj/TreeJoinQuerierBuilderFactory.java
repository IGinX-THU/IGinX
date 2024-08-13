package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder.Factory;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.Objects;

public class TreeJoinQuerierBuilderFactory implements Factory {

  private final Factory factory;

  public TreeJoinQuerierBuilderFactory(Factory factory) {
    this.factory = Objects.requireNonNull(factory);
    if (factory == this) {
      throw new IllegalArgumentException("Factory cannot be itself");
    }
  }

  @Override
  public Builder create(@Nullable String prefix, Path path, FileTreeConfig config) {
    return new TreeJoinQuerierBuilder(prefix, path, factory, config);
  }
}
