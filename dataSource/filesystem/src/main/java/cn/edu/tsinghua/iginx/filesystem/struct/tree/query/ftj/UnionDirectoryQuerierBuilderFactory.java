/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier.Builder.Factory;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnionDirectoryQuerierBuilderFactory implements Factory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnionDirectoryQuerierBuilderFactory.class);

  private final Factory factory;

  public UnionDirectoryQuerierBuilderFactory(Factory factory) {
    this.factory = Objects.requireNonNull(factory);
    if (factory == this) {
      throw new IllegalArgumentException("Factory cannot be itself");
    }
  }

  @Override
  public Builder create(
      @Nullable String prefix, Path path, FileTreeConfig config, ExecutorService executor) {
    LOGGER.debug("create tree join querier for {} at '{}' with {}", path, prefix, config);
    return new UnionDirectoryQuerierBuilder(prefix, path, factory, config, executor);
  }
}
