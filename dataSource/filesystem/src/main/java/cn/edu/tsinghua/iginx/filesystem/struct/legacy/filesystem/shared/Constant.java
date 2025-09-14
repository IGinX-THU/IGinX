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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.shared;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Constant {

  public static final String SEPARATOR = System.getProperty("file.separator");

  public static final String FILE_EXTENSION = ".iginx";

  public static final String WILDCARD = "*";

  // the number of meta info
  public static final long IGINX_FILE_META_INDEX = 3L;

  public static final int MAGIC_NUMBER_INDEX = 1;

  public static final int DATA_TYPE_INDEX = 2;

  public static final int TAG_KV_INDEX = 3;

  public static final byte[] MAGIC_NUMBER = "IGINX".getBytes();

  public static final Charset CHARSET = StandardCharsets.UTF_8;

  public static final String INIT_INFO_DIR = "dir";

  public static final String INIT_INFO_DUMMY_DIR = "dummy_dir";

  public static final String INIT_INFO_MEMORY_POOL_SIZE = "memory_pool_size";

  public static final String INIT_INFO_CHUNK_SIZE = "chunk_size_in_bytes";

  public static final String INIT_ROOT_PREFIX = "embedded_prefix";
}
