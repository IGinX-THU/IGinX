/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.common.utils;

import java.io.File;

public class FileUtils {

  public static boolean deleteFile(File file) {
    if (!file.exists()) {
      return false;
    }
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          deleteFile(f);
        }
      }
    }
    return file.delete();
  }

  public static String getLastDirName(String path) {
    String separator = System.getProperty("file.separator");
    if (!path.contains(separator)) {
      return path;
    } else if (path.endsWith(separator)) {
      String str = path.substring(0, path.lastIndexOf(separator));
      return str.substring(str.lastIndexOf(separator) + 1);
    } else {
      return path.substring(path.lastIndexOf(separator) + 1);
    }
  }
}
