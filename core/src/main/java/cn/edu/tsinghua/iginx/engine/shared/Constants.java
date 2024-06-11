/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.shared;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {

  public static final String KEY = "key";

  public static final String ORDINAL = "ordinal";

  public static final String ALL_PATH = "*";
  public static final String ALL_PATH_SUFFIX = ".*";

  public static final String UDF_CLASS = "t";
  public static final String UDF_FUNC = "transform";

  public static final String WINDOW_START_COL = "window_start";
  public static final String WINDOW_END_COL = "window_end";

  // 保留列名，会在reorder时保留，并按原顺序出现在表的最前面
  public static final Set<String> RESERVED_COLS =
      new HashSet<>(Arrays.asList(WINDOW_START_COL, WINDOW_END_COL));
}
