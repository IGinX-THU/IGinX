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
package cn.edu.tsinghua.iginx.minio;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.minio.kv.ObjectKvManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.*;
import javax.annotation.Nullable;

/** Parameters of a MinIO storage engine, parsed from extra params of {@link StorageEngineMeta}. */
public final class MinioConfig {

  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String REGION = "region";
  public static final String SECURE = "secure";
  public static final String DATA_BUCKET = "data_bucket";
  public static final String DUMMY_BUCKET = "dummy_bucket";
  public static final String DUMMY_DIR = "dummy_dir";
  public static final String EMBEDDED_PREFIX = "embedded_prefix";
  public static final String DUMMY_STRUCT = "dummy.struct";
  public static final String DUMMY_CONFIG_PREFIX = "dummy.config.";
  public static final String COMPACT_THRESHOLD = "compact_threshold";
  public static final String READ_BLOCK_SIZE = "read_block_size";
  public static final String META_CACHE_TTL_MS = "meta_cache_ttl_ms";
  public static final String CONNECT_TIMEOUT_MS = "connect_timeout_ms";
  public static final String REQUEST_TIMEOUT_MS = "request_timeout_ms";

  public static final String DEFAULT_CREDENTIAL = "minioadmin";
  public static final String DEFAULT_DATA_BUCKET = "iginx-data";
  public static final String FILE_TREE = "FileTree";
  public static final String OBJECT_KV = "ObjectKV";
  public static final String DEFAULT_DUMMY_STRUCT = FILE_TREE;

  private final String endpointHost;
  private final int endpointPort;
  private final boolean secure;
  private final String username;
  private final String password;
  private final String region;
  private final String dataBucket;
  private final String dummyBucket;
  private final String dummyDir;
  private final String dummyStruct;
  private final String embeddedPrefix;
  private final Config dummyConfig;
  private final int compactThreshold;
  private final int readBlockSize;
  private final long metaCacheTtlMs;
  private final long connectTimeoutMs;
  private final long requestTimeoutMs;
  private final boolean hasData;
  private final boolean readOnly;

  private MinioConfig(StorageEngineMeta meta) throws StorageInitializationException {
    Map<String, String> params = meta.getExtraParams();
    this.endpointHost = meta.getIp();
    this.endpointPort = meta.getPort();
    this.secure = Boolean.parseBoolean(params.getOrDefault(SECURE, "false"));
    this.username = params.getOrDefault(USERNAME, DEFAULT_CREDENTIAL);
    this.password = params.getOrDefault(PASSWORD, DEFAULT_CREDENTIAL);
    this.region = emptyToNull(params.get(REGION));
    this.dataBucket = params.getOrDefault(DATA_BUCKET, DEFAULT_DATA_BUCKET);
    this.dummyBucket = emptyToNull(params.get(DUMMY_BUCKET));
    this.dummyDir = ObjectKvManager.normalizeRoot(params.get(DUMMY_DIR));
    this.dummyStruct = params.getOrDefault(DUMMY_STRUCT, DEFAULT_DUMMY_STRUCT);
    this.compactThreshold = parseInt(params, COMPACT_THRESHOLD, 16, 2);
    this.readBlockSize = parseInt(params, READ_BLOCK_SIZE, 1024 * 1024, 4096);
    this.metaCacheTtlMs = parseInt(params, META_CACHE_TTL_MS, 2000, 0);
    this.connectTimeoutMs = parseInt(params, CONNECT_TIMEOUT_MS, 5000, 1);
    this.requestTimeoutMs = parseInt(params, REQUEST_TIMEOUT_MS, 60000, 1);
    this.hasData = meta.isHasData();
    this.readOnly = meta.isReadOnly();

    if (dataBucket.isEmpty()) {
      throw new StorageInitializationException(DATA_BUCKET + " should not be empty");
    }
    if (dataBucket.equals(dummyBucket)) {
      throw new StorageInitializationException(
          "data bucket and dummy bucket should not be the same");
    }
    if (!dummyStruct.equals(FILE_TREE) && !dummyStruct.equals(OBJECT_KV)) {
      throw new StorageInitializationException(
          "unsupported " + DUMMY_STRUCT + ": " + dummyStruct + ", expect FileTree or ObjectKV");
    }
    if (hasData && dummyBucket == null && (!dummyDir.isEmpty() || isObjectKv())) {
      throw new StorageInitializationException(
          DUMMY_BUCKET + " is required when " + DUMMY_DIR + " is set or dummy struct is ObjectKV");
    }

    Map<String, String> fileTreeParams = new HashMap<>();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (entry.getKey().startsWith(DUMMY_CONFIG_PREFIX)) {
        fileTreeParams.put(
            entry.getKey().substring(DUMMY_CONFIG_PREFIX.length()), entry.getValue());
      }
    }
    String prefix = params.get(EMBEDDED_PREFIX);
    if (prefix == null) {
      prefix = fileTreeParams.get("prefix");
    }
    if (prefix == null) {
      prefix = defaultPrefix();
    }
    this.embeddedPrefix = emptyToNull(prefix);
    fileTreeParams.remove("prefix");
    if (embeddedPrefix != null) {
      fileTreeParams.put("prefix", embeddedPrefix);
    }
    this.dummyConfig = ConfigFactory.parseMap(fileTreeParams, "minio dummy config");
  }

  public static MinioConfig of(StorageEngineMeta meta) throws StorageInitializationException {
    try {
      return new MinioConfig(meta);
    } catch (RuntimeException e) {
      throw new StorageInitializationException("invalid minio params: " + e.getMessage(), e);
    }
  }

  /** the last name of mounted path is the prefix of dummy columns */
  @Nullable
  private String defaultPrefix() {
    if (isObjectKv() || dummyBucket == null) {
      return null;
    }
    if (dummyDir.isEmpty()) {
      return dummyBucket;
    }
    String dir = dummyDir.substring(0, dummyDir.length() - 1);
    return dir.substring(dir.lastIndexOf('/') + 1);
  }

  private static int parseInt(Map<String, String> params, String key, int def, int min) {
    String value = params.get(key);
    if (value == null) {
      return def;
    }
    int parsed = Integer.parseInt(value.trim());
    if (parsed < min) {
      throw new IllegalArgumentException(key + " should not be less than " + min);
    }
    return parsed;
  }

  @Nullable
  private static String emptyToNull(@Nullable String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  public String getEndpointHost() {
    return endpointHost;
  }

  public int getEndpointPort() {
    return endpointPort;
  }

  public boolean isSecure() {
    return secure;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Nullable
  public String getRegion() {
    return region;
  }

  public String getDataBucket() {
    return dataBucket;
  }

  @Nullable
  public String getDummyBucket() {
    return dummyBucket;
  }

  /** key prefix in dummy bucket, empty or ending with "/" */
  public String getDummyDir() {
    return dummyDir;
  }

  public boolean isObjectKv() {
    return OBJECT_KV.equals(dummyStruct);
  }

  @Nullable
  public String getEmbeddedPrefix() {
    return embeddedPrefix;
  }

  public Config getDummyConfig() {
    return dummyConfig;
  }

  public int getCompactThreshold() {
    return compactThreshold;
  }

  public int getReadBlockSize() {
    return readBlockSize;
  }

  public long getMetaCacheTtlMs() {
    return metaCacheTtlMs;
  }

  public long getConnectTimeoutMs() {
    return connectTimeoutMs;
  }

  public long getRequestTimeoutMs() {
    return requestTimeoutMs;
  }

  public boolean isHasData() {
    return hasData;
  }

  public boolean isReadOnly() {
    return readOnly;
  }
}
