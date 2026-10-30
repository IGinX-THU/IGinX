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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Chooses the format of an object by its extension, objects of unknown extension are raw. */
public final class ObjectFormats {

  public static final ObjectFormat RAW = new RawChunkFormat();

  private static final Map<String, ObjectFormat> BY_EXTENSION;

  static {
    Map<String, ObjectFormat> byExtension = new HashMap<>();
    for (ObjectFormat format : Arrays.asList(RAW, new CsvFormat(), new ParquetFormat())) {
      for (String extension : format.getExtensions()) {
        byExtension.put(extension, format);
      }
    }
    BY_EXTENSION = Collections.unmodifiableMap(byExtension);
  }

  private ObjectFormats() {}

  public static ObjectFormat of(Path path) {
    String name = path.getFileName().toString();
    int dot = name.lastIndexOf('.');
    if (dot == -1) {
      return RAW;
    }
    return BY_EXTENSION.getOrDefault(name.substring(dot + 1), RAW);
  }
}
