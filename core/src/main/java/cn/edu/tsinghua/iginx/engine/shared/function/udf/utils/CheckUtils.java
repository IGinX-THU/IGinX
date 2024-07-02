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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import java.util.ArrayList;
import java.util.List;

public class CheckUtils {

  public static <T> List<T> castList(Object obj, Class<T> clazz) {
    List<T> result = new ArrayList<T>();
    if (obj instanceof List<?>) {
      for (Object o : (List<?>) obj) {
        result.add(clazz.cast(o));
      }
      return result;
    }
    return null;
  }

  public static boolean isLegal(FunctionParams params) {
    List<String> paths = params.getPaths();
    return paths != null && !paths.isEmpty();
  }
}
