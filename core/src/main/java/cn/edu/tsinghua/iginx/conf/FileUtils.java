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
package cn.edu.tsinghua.iginx.conf;

import java.io.File;

public class FileUtils {

  public static boolean isAbsolutePath(String path) {
    File file = new File(path);

    // 判断是否为绝对路径
    if (file.isAbsolute()) {
      return true;
    }

    // 如果不是绝对路径，再根据操作系统类型判断
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      return path.matches("^([a-zA-Z]:\\\\|\\\\).*");
    } else {
      return path.startsWith("/");
    }
  }
}
