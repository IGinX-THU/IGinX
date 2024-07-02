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
