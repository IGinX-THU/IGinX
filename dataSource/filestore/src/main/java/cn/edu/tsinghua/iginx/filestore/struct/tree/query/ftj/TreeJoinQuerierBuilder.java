package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.IginxPaths;
import cn.edu.tsinghua.iginx.filestore.common.Patterns;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Queriers;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TreeJoinQuerierBuilder implements Builder {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeJoinQuerierBuilder.class);

  private final String prefix;
  private final Path path;
  private final Factory factory;
  private final FileTreeConfig config;

  TreeJoinQuerierBuilder(
      @Nullable String prefix, Path path, Factory factory, FileTreeConfig config) {
    this.prefix = prefix;
    this.path = path;
    this.factory = factory;
    this.config = config;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public String toString() {
    return "TreeJoinQuerierBuilder{"
        + "prefix='"
        + prefix
        + '\''
        + ", path="
        + path
        + ", factory="
        + factory
        + ", config="
        + config
        + '}';
  }

  @Override
  public Querier build(DataTarget target) throws IOException {
    LOGGER.debug("{} enter {} at '{}'", target, path, prefix);
    boolean needPostFilter = false;

    TreeJoinQuerier treeJoinQuerier = new TreeJoinQuerier(path, prefix, target);
    try (DirectoryStream<Path> children = Files.newDirectoryStream(path)) {
      for (Path subpath : children) {
        String subPrefix =
            IginxPaths.join(prefix, IginxPaths.get(subpath.getFileName(), config.getDot()));

        List<String> subPatterns = Patterns.filterByPrefix(target.getPatterns(), subPrefix);
        if (Patterns.isEmpty(subPatterns)) {
          LOGGER.debug("Skip {} with {} due to no pattern match", subpath, subPrefix);
          continue;
        }

        Predicate<Filter> subFilterTester = Filters.startWith(subPrefix);
        Filter subFilter = Filters.superSet(target.getFilter(), subFilterTester);
        if (!Filters.match(target.getFilter(), subFilterTester)) {
          needPostFilter = true;
        }

        DataTarget subTarget = target.withPatterns(subPatterns).withFilter(subFilter);

        try (Builder subBuilder = factory.create(subPrefix, subpath, config)) {
          Querier subQuerier = subBuilder.build(subTarget);
          treeJoinQuerier.add(subQuerier);
        }
      }
    } catch (IOException e) {
      treeJoinQuerier.close();
      throw e;
    }

    if (!needPostFilter) {
      return treeJoinQuerier;
    }
    LOGGER.debug("set post filter for {}", target);
    return Queriers.filtered(treeJoinQuerier, target.getFilter());
  }
}
