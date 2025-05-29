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

import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier.Builder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;

class FormatQuerierBuilder implements Builder {

  private final String prefix;
  private final Path path;
  private final FileFormat format;
  private final Config config;
  private final ExecutorService executor;

  FormatQuerierBuilder(
      @Nullable String prefix,
      Path path,
      FileFormat format,
      Config config,
      ExecutorService executor) {
    this.format = format;
    this.prefix = prefix;
    this.path = path;
    this.config = config;
    this.executor = executor;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Querier build(DataTarget subTarget) throws IOException {
    FileFormat.Reader reader = format.newReader(prefix, path, config);
    return new FormatQuerier(path, prefix, subTarget, reader, executor);
  }
}
