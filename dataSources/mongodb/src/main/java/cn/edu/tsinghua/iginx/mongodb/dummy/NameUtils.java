/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
