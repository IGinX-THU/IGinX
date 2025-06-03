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
package cn.edu.tsinghua.iginx.filesystem.format.csv;

import cn.edu.tsinghua.iginx.filesystem.format.AbstractFileFormat;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.Nullable;

@AutoService(FileFormat.class)
public class CsvFormat extends AbstractFileFormat {

  public static final String NAME = "CSV";

  public CsvFormat() {
    super(NAME, "csv", "tsv");
  }

  @Override
  public String getName() {
    return formatName;
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    CsvReaderConfig csvReaderConfig = CsvReaderConfig.of(config);
    return new CsvReader(prefix, path, csvReaderConfig);
  }
}
