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
package cn.edu.tsinghua.iginx.transform.utils;

import java.util.HashMap;
import java.util.Map;

public class Constants {

  public static final String KEY = "key";

  public static final String PARAM_PATHS = "param_paths";
  public static final String PARAM_LEVELS = "param_levels";
  public static final String PARAM_MODULE = "param_module";
  public static final String PARAM_CLASS = "param_class";

  public static final String UDF_CLASS = "t";
  public static final String UDF_FUNC = "transform";

  public static final Map<Integer, String> WORKER_STATUS_MAP = new HashMap<>();

  static {
    WORKER_STATUS_MAP.put(0, "SUCCESS");
    WORKER_STATUS_MAP.put(-1, "FAIL_TO_CREATE_SOCKET");
    WORKER_STATUS_MAP.put(-2, "FAIL_TO_BIND_ADDR");
    WORKER_STATUS_MAP.put(-3, "FAIL_TO_LOAD_CLASS");
  }

  public static String getWorkerStatusInfo(int status) {
    return WORKER_STATUS_MAP.get(status);
  }
}
