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
package cn.edu.tsinghua.iginx.filesystem.format.raw;

import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

@AutoService(FileFormat.class)
public class RawFormat implements FileFormat {

  public static final String NAME = "RawChunk";

  public static final RawFormat INSTANCE = new RawFormat();

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
    return Collections.emptyList();
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    RawReaderConfig rawReaderConfig = RawReaderConfig.of(config);
    return new RawReader(String.valueOf(prefix), path, rawReaderConfig);
  }
}
