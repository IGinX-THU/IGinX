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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.github.luben.zstd.Zstd;
import com.google.common.collect.Range;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.paimon.shade.org.apache.parquet.format.CompressionCodec;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ParquetConfig extends AbstractConfig {

  @Optional int readBatchSize = 1024;

  @Optional int writeBatchSize = 1024;

  @Optional ConfigMemorySize writeBatchMemory = ConfigMemorySize.ofBytes(128 * 1024 * 1024);

  // paimon's parquet 读取器在使用 lz4_raw 过滤 blocks 读取字典时会有 BUG，出现崩溃
  @Optional CompressionCodec compression = CompressionCodec.UNCOMPRESSED;

  @Optional int zstdLevel = Zstd.defaultCompressionLevel();

  @Optional IndexConfig index = new IndexConfig();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateInRange(
        problems,
        Fields.zstdLevel,
        Range.closed(Zstd.minCompressionLevel(), Zstd.maxCompressionLevel()),
        zstdLevel);
    validateSubConfig(problems, Fields.index, index);
    return problems;
  }

  public static ParquetConfig of(Config config) {
    return of(config, ParquetConfig.class);
  }
}
