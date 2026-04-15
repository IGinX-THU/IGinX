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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow.ArrowFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet.ParquetFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile.TsfileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;
import java.util.Objects;
import java.util.function.BiFunction;

public enum FileFormatType {
  PARQUET(ParquetFormat::new),
  TSFILE(TsfileFormat::new),
  ARROW(ArrowFormat::new);

  interface FileFormatFactory {
    ImmutableFileFormat create(Config config, CachePool cachePool, Indexer indexer);
  }

  private final FileFormatFactory factory;

  FileFormatType(FileFormatFactory factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  public ImmutableFileFormat create(Config config, CachePool cachePool, Indexer indexer) {
    return factory.create(config, cachePool, indexer);
  }
}
