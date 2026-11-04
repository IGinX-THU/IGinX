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
package cn.edu.tsinghua.iginx.minio.tree.format;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * CSV and TSV objects with a header line. Config (all optional):
 *
 * <ul>
 *   <li>{@code delimiter}: default "," for .csv and tab for .tsv
 *   <li>{@code parseTypeFromHeader}: parse "name:type" in header, default true
 *   <li>{@code inferSchema}: infer types of untyped columns from the first {@code sampleSize} rows,
 *       default false (all BINARY)
 *   <li>{@code sampleSize}: default 1
 *   <li>{@code dateFormat}: default "yyyy-MM-dd", dates are read as LONG milliseconds
 *   <li>{@code allowDuplicateColumnNames}: rename duplicate columns to name_1, name_2..., default
 *       false
 * </ul>
 */
final class CsvFormat implements ObjectFormat {

  static final String NAME = "CSV";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Arrays.asList("csv", "tsv");
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    return new CsvReader(prefix, path, CsvReader.Options.of(config));
  }

  @Override
  public String toString() {
    return NAME;
  }
}
