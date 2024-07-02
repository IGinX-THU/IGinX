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
package cn.edu.tsinghua.iginx.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

  public static String convertToString(String filePath) {
    String conf = null;
    InputStream in = null;
    try {
      in = new BufferedInputStream(Files.newInputStream(Paths.get(filePath)));
      conf = IOUtils.toString(in, String.valueOf(StandardCharsets.UTF_8)).replace("\n", "");
    } catch (IOException e) {
      LOGGER.warn("Fail to find file, path={}", filePath);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOGGER.error("Fail to close the file, path={}", filePath);
      }
    }
    return conf;
  }
}
