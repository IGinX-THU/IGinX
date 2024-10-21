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
package cn.edu.tsinghua.iginx.filesystem.struct;

import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.concurrent.Immutable;

@Immutable
public interface FileStructure {
  String getName();

  Closeable newShared(Config config) throws IOException;

  FileManager newReader(Path path, Closeable shared) throws IOException;

  boolean supportWrite();

  FileManager newWriter(Path path, Closeable shared) throws IOException;
}
