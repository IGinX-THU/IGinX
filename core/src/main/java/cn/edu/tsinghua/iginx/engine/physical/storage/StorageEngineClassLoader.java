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

public class StorageEngineClassLoader extends ClassLoader {

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
                jarFile.stream().map(JarEntry::getName)
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
                ByteArrayOutputStream os = new ByteArrayOutputStream(); // Note: Closing a ByteArrayOutputStream has no effect
                byte[] buffer = new byte[8192];
                int read;
                while ((read = is.read(buffer)) >= 0) {
                    os.write(buffer, 0, read);
                }
                byte[] b = os.toByteArray();
                return defineClass(name, b, 0, b.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                e.printStackTrace();
            }
        }
        return null;
    }
}
