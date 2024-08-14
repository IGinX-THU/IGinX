package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder.Factory;
import java.nio.file.Path;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeJoinQuerierBuilderFactory implements Factory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeJoinQuerierBuilderFactory.class);

  private final Factory factory;

  public TreeJoinQuerierBuilderFactory(Factory factory) {
    this.factory = Objects.requireNonNull(factory);
    if (factory == this) {
      throw new IllegalArgumentException("Factory cannot be itself");
    }
  }

  @Override
  public Builder create(@Nullable String prefix, Path path, FileTreeConfig config) {
    LOGGER.debug("create tree join querier for {} at '{}' with {}", path, prefix, config);
    return new TreeJoinQuerierBuilder(prefix, path, factory, config);
  }
}
