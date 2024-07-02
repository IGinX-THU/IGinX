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
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageEngineClassLoader extends ClassLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageEngineClassLoader.class);

  private final File[] Jars;

  private final Map<String, File> nameToJar;

  private final Map<String, Class<?>> classMap = new ConcurrentHashMap<>();

  public StorageEngineClassLoader(String path) throws IOException {
    File dir = new File(EnvUtils.loadEnv(Constants.DRIVER, Constants.DRIVER_DIR), path);
    this.Jars = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".jar"));
    this.nameToJar = new HashMap<>();
    preloadClassNames();
  }

  private void preloadClassNames() throws IOException {
    if (Jars == null) {
      return; // Instantiation of unused driver ClassLoader is not an error
    }
    for (File jar : Jars) {
      try (JarFile jarFile = new JarFile(jar)) {
        jarFile.stream()
            .map(JarEntry::getName)
            .filter(name -> name.endsWith(".class"))
            .map(classFileName -> classFileName.replace(".class", "").replace('/', '.'))
            .forEach(className -> nameToJar.put(className, jar));
      }
    }
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (classMap.containsKey(name)) {
      return classMap.get(name);
    }

    Class<?> clazz = findClass(name);
    if (clazz != null) {
      if (resolve) {
        resolveClass(clazz);
      }
    } else {
      clazz = super.loadClass(name, resolve);
    }
    classMap.put(name, clazz);
    return clazz;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    File jar = nameToJar.get(name);
    if (jar == null) {
      return null;
    }
    try (JarFile jarFile = new JarFile(jar)) {
      String classFileName = name.replace('.', '/') + ".class";
      try (InputStream is = jarFile.getInputStream(jarFile.getEntry(classFileName))) {
        // Since Java 9, there are readAllBytes and transferTo
        // However, we need to be compatible with Java 8
        ByteArrayOutputStream os =
            new ByteArrayOutputStream(); // Note: Closing a ByteArrayOutputStream has no
        // effect
        byte[] buffer = new byte[8192];
        int read;
        while ((read = is.read(buffer)) >= 0) {
          os.write(buffer, 0, read);
        }
        byte[] b = os.toByteArray();
        return defineClass(name, b, 0, b.length);
      }
    } catch (IOException e) {
      LOGGER.error("unexpected error: ", e);
    }
    return null;
  }

  @Override
  public URL getResource(String name) {
    URL res = findResource(name);
    if (res != null) {
      return res;
    }
    return super.getResource(name);
  }

  @Override
  protected URL findResource(String name) {
    for (File jar : Jars) {
      try (JarFile jarFile = new JarFile(jar)) {
        if (jarFile.getJarEntry(name) != null) {
          return new URL("jar:" + jar.toURI().toURL().toString() + "!/" + name);
        }
      } catch (IOException e) {
        LOGGER.error("unexpected error: ", e);
      }
    }
    return null;
  }
}
