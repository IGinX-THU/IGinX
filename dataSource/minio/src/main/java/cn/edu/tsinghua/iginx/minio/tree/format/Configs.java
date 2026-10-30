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
import javax.annotation.Nullable;

/** Optional values of a format config, values given as strings are converted. */
final class Configs {

  private Configs() {}

  static boolean getBoolean(Config config, String path, boolean def) {
    return config.hasPath(path) ? config.getBoolean(path) : def;
  }

  static int getInt(Config config, String path, int def) {
    return config.hasPath(path) ? config.getInt(path) : def;
  }

  @Nullable
  static String getString(Config config, String path, @Nullable String def) {
    return config.hasPath(path) ? config.getString(path) : def;
  }

  /** size in bytes, units like "4KB" are supported */
  static long getBytes(Config config, String path, long def) {
    return config.hasPath(path) ? config.getMemorySize(path).toBytes() : def;
  }
}
