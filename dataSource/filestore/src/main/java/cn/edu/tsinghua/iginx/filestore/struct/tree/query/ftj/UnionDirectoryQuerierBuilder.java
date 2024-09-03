/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
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
import java.util.*;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UnionDirectoryQuerierBuilder implements Builder {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnionDirectoryQuerierBuilder.class);

  private final String prefix;
  private final Path path;
  private final Factory factory;
  private final FileTreeConfig config;

  UnionDirectoryQuerierBuilder(
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

    Map<String, Path> matchedChildren = matchedChildren(target);

    boolean needPostFilter = false;
    List<Querier> subQueriers = new ArrayList<>();
    try {
      for (Map.Entry<String, Path> entry : matchedChildren.entrySet()) {
        String subPrefix = entry.getKey();
        Path subPath = entry.getValue();

        DataTarget subTarget = extractTarget(target, subPrefix);
        try (Builder subBuilder = factory.create(subPrefix, subPath, config)) {
          Querier subQuerier = subBuilder.build(subTarget);
          subQueriers.add(subQuerier);
        }
        if (!Filters.match(target.getFilter(), Filters.startWith(subPrefix))) {
          needPostFilter = true;
        }
      }
    } catch (IOException e) {
      Closeables.close(subQueriers);
      throw e;
    }

    UnionDirectoryQuerier unionDirectoryQuerier =
        new UnionDirectoryQuerier(path, prefix, target, subQueriers);
    if (!needPostFilter) {
      return unionDirectoryQuerier;
    }
    LOGGER.debug("set post filter for {}", target);
    return Queriers.filtered(unionDirectoryQuerier, target.getFilter());
  }

  private DataTarget extractTarget(DataTarget target, String subPrefix) {
    List<String> subPatterns = Patterns.filterByPrefix(target.getPatterns(), subPrefix);
    Filter subFilter = Filters.superSet(target.getFilter(), Filters.startWith(subPrefix));
    return target.withPatterns(subPatterns).withFilter(subFilter);
  }

  private String subPrefix(String prefix, Path subpath) {
    return IginxPaths.join(prefix, IginxPaths.get(subpath.getFileName(), config.getDot()));
  }

  private Map<String, Path> matchedChildren(DataTarget target) throws IOException {
    HashMap<String, Path> matchedChildren = new LinkedHashMap<>();
    for (String pattern : Patterns.nullToAll(target.getPatterns())) {
      if (!Patterns.startsWith(pattern, prefix)) {
        continue;
      }
      String patternSuffix = Patterns.suffix(pattern, prefix);
      String[] subPatterns = IginxPaths.split(patternSuffix);
      if (subPatterns.length == 0) {
        continue;
      }

      String nextPatternNode = subPatterns[0];
      if (Patterns.isWildcard(nextPatternNode)) {
        return allChildren();
      }

      Path relativePath =
          IginxPaths.toFilePath(nextPatternNode, config.getDot(), path.getFileSystem());
      Path subpath = path.resolve(relativePath);
      if (!Files.exists(subpath)) {
        continue;
      }

      String subPrefix = subPrefix(prefix, subpath);
      matchedChildren.put(subPrefix, subpath);
    }
    return matchedChildren;
  }

  private Map<String, Path> allChildren() throws IOException {
    HashMap<String, Path> matchedChildren = new LinkedHashMap<>();
    try (DirectoryStream<Path> children = Files.newDirectoryStream(path)) {
      for (Path child : children) {
        String subPrefix = subPrefix(prefix, child);
        matchedChildren.put(subPrefix, child);
      }
    }
    return matchedChildren;
  }
}
