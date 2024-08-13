package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.common.DataTargets;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.IginxPaths;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Queriers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class TreeJoinQuerierBuilder implements Builder {

  private final String prefix;
  private final Path path;
  private final Factory factory;
  private final FileTreeConfig config;

  TreeJoinQuerierBuilder(@Nullable String prefix, Path path, Factory factory, FileTreeConfig config) {
    this.prefix = prefix;
    this.path = path;
    this.factory = factory;
    this.config = config;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public String toString() {
    return "TreeJoinQuerierBuilder{" +
        "prefix='" + prefix + '\'' +
        ", path=" + path +
        ", factory=" + factory +
        ", config=" + config +
        '}';
  }

  @Override
  public Querier build(DataTarget target) throws IOException {
    List<Querier> queriers = new ArrayList<>();
    try (DirectoryStream<Path> children = Files.newDirectoryStream(path)) {
      for (Path subpath : children) {
        // TODO: 跳过不可能的文件
        String subPrefix = IginxPaths.get(prefix, IginxPaths.get(subpath.getFileName(), config.getDot()));
        DataTarget subTarget = DataTargets.subTarget(target, subPrefix);
        try (Builder subBuilder = factory.create(subPrefix, subpath, config)) {
          Querier querier = subBuilder.build(subTarget);
          ;
          queriers.add(querier);
        }
      }
    } catch (IOException e) {
      Closeables.close(queriers);
      throw e;
    }

    // TODO: 何时进行后过滤？
    TreeJoinQuerier querier = new TreeJoinQuerier(queriers);
    if (queriers.size() <= 1 || Filters.match(target.getFilter(), Filters.nonKeyFilter())) {
      return querier;
    }
    return Queriers.filtered(querier, target.getFilter());
  }

}
