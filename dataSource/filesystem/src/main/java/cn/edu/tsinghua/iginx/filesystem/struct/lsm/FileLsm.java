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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm;

import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.FileStructure;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.Shared;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(FileStructure.class)
public class FileLsm implements FileStructure {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLsm.class);

  public static final String NAME = "FileLsm";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    FileLsmConfig fileLsmConfig = FileLsmConfig.of(config);
    LOGGER.info("storage config: {}", fileLsmConfig);

    return Shared.of(fileLsmConfig);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportWrite() {
    return true;
  }

  @Override
  public FileManager newWriter(Path path, Closeable shared) throws IOException {
    return new FileLsmManager((Shared) shared, path);
  }
}
