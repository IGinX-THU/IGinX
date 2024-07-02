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
package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.LENGTH_OF_SEQUENCE_NUMBER;

import cn.edu.tsinghua.iginx.conf.Constants;

public class IdUtils {

  public static String generateId(String prefix, long id) {
    return prefix + String.format("%0" + LENGTH_OF_SEQUENCE_NUMBER + "d", id);
  }

  public static String generateDummyStorageUnitId(long id) {
    return generateId(Constants.DUMMY, id);
  }
}
