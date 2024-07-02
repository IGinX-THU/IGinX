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
package cn.edu.tsinghua.iginx.transform.utils;

import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedirectLogger extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedirectLogger.class);

  private final InputStream inputStream;

  private final String name;

  public RedirectLogger(InputStream inputStream, String name) {
    this.inputStream = inputStream;
    this.name = name;
  }

  @Override
  public void run() {
    LOGGER.info("hello");
    //        Scanner scanner = new Scanner(inputStream);
    //        while (scanner.hasNextLine()) {
    //            LOGGER.info(String.format("[Python %s] ", name) + scanner.nextLine());
    //        }
    try {
      byte[] buffer = new byte[1024];
      int len = -1;
      while ((len = inputStream.read(buffer)) > 0) {
        System.out.write(buffer, 0, len);
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
  }
}
