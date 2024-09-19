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
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;

@AutoService(FileFormat.class)
public class ParquetFormat implements FileFormat {

  public static final String NAME = "Parquet";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Collections.singletonList("parquet");
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    IParquetReader.Builder builder = IParquetReader.builder(path);
    ParquetMetadata footer;
    try (IParquetReader reader = builder.build()) {
      footer = reader.getMeta();
    }

    return new ParquetFormatReader(prefix, builder, footer);
  }
}
