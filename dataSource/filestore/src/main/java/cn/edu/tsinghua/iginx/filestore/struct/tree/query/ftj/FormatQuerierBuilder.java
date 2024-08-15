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
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Queriers;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.Nullable;

class FormatQuerierBuilder implements Builder {

  @Nullable private final String prefix;
  private final Path path;
  private final FileFormat format;
  private final Config config;

  FormatQuerierBuilder(@Nullable String prefix, Path path, FileFormat format, Config config) {
    this.format = format;
    this.prefix = prefix;
    this.path = path;
    this.config = config;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Querier build(DataTarget target) throws IOException {

    Filter filter = Filters.superSet(target.getFilter(), Filters.nonKeyFilter());

    FileFormat.Reader reader = format.newReader(prefix, path, config);

    FormatQuerier querier = new FormatQuerier(path, prefix, target, reader);
    if (Filters.match(target.getFilter(), Filters.nonKeyFilter())) {
      return querier;
    }
    return Queriers.filtered(querier, target.getFilter());
  }
}
