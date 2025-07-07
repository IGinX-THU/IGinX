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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Constants {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public static final String[] PYTHON_PATHS =
      new String[] {
        String.join(File.separator, config.getDefaultUDFDir(), "python_scripts") // scripts
      }; // tools

  public static final String SCRIPTS_PATH = PYTHON_PATHS[0];

  public static final String TIMEOUT_SCRIPT = "timeout_handler";
  public static final String TIMEOUT_WRAPPER = "TimeoutSafeWrapper";

  public static final String IMPORT_SENTINEL_SCRIPT = "import_sentinel";
  public static final String GET_FILE_MODULES_METHOD = "get_import_from_file";
  public static final String BLOCK_MODULES_METHOD = "block_imports";
  public static final List<String> MODULES_TO_BLOCK = Arrays.asList("iginx_pyclient");
}
