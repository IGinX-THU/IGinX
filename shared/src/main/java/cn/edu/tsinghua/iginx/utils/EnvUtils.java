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

        return Boolean.parseBoolean(env);
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
            LOGGER.error("unexpected long env: {} = {}", name, env, e);
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
            LOGGER.error("unexpected int env: {} = {}", name, env, e);
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
            LOGGER.error("unexpected double env: {} = {}", name, env, e);
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
            LOGGER.error("unexpected float env: {} = {}", name, env, e);
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
