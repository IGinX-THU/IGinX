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
