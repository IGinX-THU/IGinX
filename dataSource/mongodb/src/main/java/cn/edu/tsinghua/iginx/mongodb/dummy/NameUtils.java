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
package cn.edu.tsinghua.iginx.mongodb.dummy;

class NameUtils {
  static String getSuffix(String path, String prefix) {
    if (path.startsWith(prefix + ".")) {
      return path.substring(prefix.length() + 1);
    }
    throw new IllegalArgumentException(prefix + " is not prefix of " + path);
  }

  static boolean containNumberNode(String path) {
    for (String node : path.split("\\.")) {
      if (node.chars().allMatch(Character::isDigit)) {
        return true;
      }
    }
    return false;
  }
}
