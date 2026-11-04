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
package cn.edu.tsinghua.iginx.minio.tree;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.ESCAPED_DOT;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import javax.annotation.Nullable;

/**
 * Config of the FileTree and LegacyFilesystem dummy structures, parsed from {@code dummy.config.*}:
 *
 * <ul>
 *   <li>{@code dot}: how a dot in object names is written in column names, default {@code \.}
 *   <li>{@code prefix}: prefix of all columns, null means no prefix
 *   <li>{@code boundary_level}: 0 to report the prefix as the column boundary, 1 to report the
 *       range of children names
 *   <li>{@code formats.<format>.*}: config of each format, such as {@code formats.CSV.inferSchema}
 *   <li>{@code chunk_size_in_bytes}: size of chunks when objects are read as raw bytes in the
 *       LegacyFilesystem structure, default 1048576 as filesystem
 * </ul>
 */
public final class ObjectTreeConfig {

  public static final String DOT = "dot";
  public static final String PREFIX = "prefix";
  public static final String BOUNDARY_LEVEL = "boundary_level";
  public static final String FORMATS = "formats";
  public static final String CHUNK_SIZE_IN_BYTES = "chunk_size_in_bytes";

  private static final long DEFAULT_CHUNK_SIZE_IN_BYTES = 1024 * 1024;

  private final String dot;
  @Nullable private final String prefix;
  private final int boundaryLevel;
  private final int chunkSizeInBytes;
  private final Config formats;

  private ObjectTreeConfig(Config config) {
    this.dot = config.hasPath(DOT) ? config.getString(DOT) : ESCAPED_DOT;
    this.prefix = config.hasPath(PREFIX) ? config.getString(PREFIX) : null;
    this.boundaryLevel = config.hasPath(BOUNDARY_LEVEL) ? config.getInt(BOUNDARY_LEVEL) : 0;
    if (boundaryLevel != 0 && boundaryLevel != 1) {
      throw new IllegalArgumentException(BOUNDARY_LEVEL + " must be 0 or 1");
    }
    long chunkSize =
        config.hasPath(CHUNK_SIZE_IN_BYTES)
            ? config.getMemorySize(CHUNK_SIZE_IN_BYTES).toBytes()
            : DEFAULT_CHUNK_SIZE_IN_BYTES;
    if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          CHUNK_SIZE_IN_BYTES + " should be in (0, 2GB): " + chunkSize);
    }
    this.chunkSizeInBytes = (int) chunkSize;
    Config formats = ConfigFactory.empty();
    if (config.hasPath(FORMATS)) {
      ConfigValue value = config.getValue(FORMATS);
      if (value.valueType() == ConfigValueType.OBJECT) {
        formats = config.getConfig(FORMATS);
      }
    }
    this.formats = formats;
  }

  public static ObjectTreeConfig of(Config config) {
    return new ObjectTreeConfig(config);
  }

  public String getDot() {
    return dot;
  }

  @Nullable
  public String getPrefix() {
    return prefix;
  }

  public int getBoundaryLevel() {
    return boundaryLevel;
  }

  public int getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }

  /** config of a format by its name, empty if not given */
  public Config getFormatConfig(String format) {
    ConfigValue value = formats.root().get(format);
    if (value == null || value.valueType() != ConfigValueType.OBJECT) {
      return ConfigFactory.empty();
    }
    return formats.getConfig("\"" + format + "\"");
  }

  @Override
  public String toString() {
    return "ObjectTreeConfig{dot='"
        + dot
        + "', prefix="
        + prefix
        + ", boundaryLevel="
        + boundaryLevel
        + ", chunkSizeInBytes="
        + chunkSizeInBytes
        + ", formats="
        + formats.root().render()
        + '}';
  }
}
