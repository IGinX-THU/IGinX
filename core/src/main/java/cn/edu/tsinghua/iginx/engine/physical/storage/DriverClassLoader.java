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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverClassLoader extends URLClassLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DriverClassLoader.class);

  public static DriverClassLoader of(String engineName) throws IOException {
    Path path = Paths.get(EnvUtils.loadEnv(Constants.DRIVER, Constants.DRIVER_DIR), engineName);
    List<URL> urls = new ArrayList<>();
    urls.add(path.toUri().toURL());
    // Load all jar files in the driver directory
    if (Files.isDirectory(path)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.jar")) {
        for (Path jar : stream) {
          LOGGER.debug("register jar for driver {}: {}", engineName, jar);
          urls.add(jar.toUri().toURL());
        }
      }
    }
    return new DriverClassLoader(urls.toArray(new URL[0]));
  }

  public DriverClassLoader(URL[] urls) {
    super(urls);
  }
}
