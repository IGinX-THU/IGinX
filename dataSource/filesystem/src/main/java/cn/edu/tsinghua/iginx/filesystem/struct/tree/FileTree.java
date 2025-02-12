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
package cn.edu.tsinghua.iginx.filesystem.struct.tree;

import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.FileStructure;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(FileStructure.class)
public class FileTree implements FileStructure {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTree.class);

  public static final String NAME = "FileTree";

  @Override
  public String getName() {
    return NAME;
  }

  @Value
  private static class Shared implements Closeable {

    FileTreeConfig config;

    @Override
    public void close() throws IOException {}
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    LOGGER.debug("Create shared instance with config: {}", config);
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(config);
    return new Shared(fileTreeConfig);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    LOGGER.debug("Create reader with path: {}", path);
    return new FileTreeManager(path, ((Shared) shared).getConfig());
  }

  @Override
  public boolean supportWrite() {
    return false;
  }

  @Override
  public FileManager newWriter(Path path, Closeable shared) throws IOException {
    throw new UnsupportedOperationException();
  }
}
