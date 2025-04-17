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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import static cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.Constants.GET_FILE_MODULES_METHOD;
import static cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.Constants.IMPORT_SENTINEL_SCRIPT;
import static cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.Constants.MODULES_TO_BLOCK;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  public static Set<String> importCheck(String filepath) {
    ThreadInterpreterManager.exec(
        String.format("from %s import %s", IMPORT_SENTINEL_SCRIPT, GET_FILE_MODULES_METHOD));
    List<String> modules =
        (List<String>)
            ThreadInterpreterManager.executeWithInterpreterAndReturn(
                interpreter -> interpreter.invoke(GET_FILE_MODULES_METHOD, filepath));
    Set<String> res = new HashSet<>();
    for (String module : MODULES_TO_BLOCK) {
      if (modules.contains(module)) {
        res.add(module);
      }
    }
    return res;
  }
}
