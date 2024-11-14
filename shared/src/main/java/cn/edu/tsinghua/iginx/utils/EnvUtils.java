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
package cn.edu.tsinghua.iginx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvUtils.class);

  public static boolean loadEnv(String name, boolean defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    try {
      return Boolean.parseBoolean(env);
    } catch (NumberFormatException e) {
      LOGGER.error("unexpected boolean env: {} = {}", name, env);
      return defaultValue;
    }
  }

  public static long loadEnv(String name, long defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(env);
    } catch (NumberFormatException e) {
      LOGGER.error("unexpected long env: {} = {}", name, env);
      return defaultValue;
    }
  }

  public static int loadEnv(String name, int defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(env);
    } catch (NumberFormatException e) {
      LOGGER.error("unexpected int env: {} = {}", name, env);
      return defaultValue;
    }
  }

  public static double loadEnv(String name, double defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(env);
    } catch (NumberFormatException e) {
      LOGGER.error("unexpected double env: {} = {}", name, env);
      return defaultValue;
    }
  }

  public static float loadEnv(String name, float defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    try {
      return Float.parseFloat(env);
    } catch (NumberFormatException e) {
      LOGGER.error("unexpected float env: {} = {}", name, env);
      return defaultValue;
    }
  }

  public static String loadEnv(String name, String defaultValue) {
    String env = System.getProperty(name);
    env = env == null ? System.getenv(name) : env;
    if (env == null) {
      return defaultValue;
    }
    return env;
  }
}
