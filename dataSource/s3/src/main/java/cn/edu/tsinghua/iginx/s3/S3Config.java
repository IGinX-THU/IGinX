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
package cn.edu.tsinghua.iginx.s3;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.ESCAPED_DOT;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.HostPolicy;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.s3.common.IginxPaths;
import cn.edu.tsinghua.iginx.s3.kv.ObjectKvManager;
import cn.edu.tsinghua.iginx.s3.strategy.S3StrategyFactory;
import cn.edu.tsinghua.iginx.s3.tree.ObjectTreeConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import javax.annotation.Nullable;

/**
 * Parameters of an S3 storage engine, parsed from extra params of {@link StorageEngineMeta}.
 *
 * <p>Service capabilities come only from bundled {@code <engine>-meta.properties} overlaid on
 * {@code default-meta.properties}. Registration options cannot override them.
 */
public final class S3Config {

  public static final String ENGINE = "engine";
  public static final String VIRTUAL_STYLE = "virtual_style";
  public static final String REGION_REQUIRED = "region_required";
  public static final String AUTO_CREATE_BUCKET = "auto_create_bucket";
  private static final String HOST_POLICY = "host_policy";

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
  public static final String LEGACY_FILESYSTEM = "LegacyFilesystem";
  public static final String FILE_TREE = "FileTree";
  public static final String OBJECT_KV = "ObjectKV";
  /** same as filesystem: objects are read as raw bytes unless FileTree is chosen */
  public static final String DEFAULT_DUMMY_STRUCT = LEGACY_FILESYSTEM;

  /** meta files of other drivers on the same classpath (relational) have the same names */
  public static final String META_DIR = "s3/";

  public static final String DEFAULT_META_FILE = "default-meta.properties";
  public static final String META_FILE_SUFFIX = "-meta.properties";
  private static final List<String> UNSUPPORTED_OPTIONS =
      Collections.unmodifiableList(
          Arrays.asList(
              "meta_properties_path",
              "preserve_host",
              HOST_POLICY,
              SECURE,
              VIRTUAL_STYLE,
              REGION_REQUIRED,
              AUTO_CREATE_BUCKET));

  private final Map<String, String> params;
  private final String engine;
  private final String endpointHost;
  private final int endpointPort;
  private final boolean secure;
  private final boolean virtualStyle;
  private final boolean regionRequired;
  private final boolean autoCreateBucket;
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

  private S3Config(
      String ip, int port, Map<String, String> params, boolean hasData, boolean readOnly)
      throws StorageInitializationException, IOException {
    this.params = Collections.unmodifiableMap(new HashMap<>(params));
    String engineParam = emptyToNull(params.get(ENGINE));
    if (engineParam == null) {
      throw new StorageInitializationException(
          ENGINE + " is required, expect one of " + S3StrategyFactory.ENGINES);
    }
    this.engine = engineParam.trim().toLowerCase(Locale.ROOT);
    if (!S3StrategyFactory.ENGINES.contains(engine)) {
      throw new StorageInitializationException(
          "unsupported "
              + ENGINE
              + ": "
              + engineParam
              + ", expect one of "
              + S3StrategyFactory.ENGINES);
    }
    for (String key : params.keySet()) {
      if (UNSUPPORTED_OPTIONS.contains(key.toLowerCase(Locale.ROOT))) {
        throw new StorageInitializationException(
            key + " is not supported; S3 service capabilities are defined by the driver");
      }
    }
    Properties meta = loadMeta(engine);
    this.secure = parseBoolean(meta, SECURE);
    this.virtualStyle = parseBoolean(meta, VIRTUAL_STYLE);
    this.regionRequired = parseBoolean(meta, REGION_REQUIRED);
    this.autoCreateBucket = parseBoolean(meta, AUTO_CREATE_BUCKET);
    this.endpointHost = ip;
    this.endpointPort = port;
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
    this.hasData = hasData;
    this.readOnly = readOnly;

    if (dataBucket.isEmpty()) {
      throw new StorageInitializationException(DATA_BUCKET + " should not be empty");
    }
    if (dataBucket.equals(dummyBucket)) {
      throw new StorageInitializationException(
          "data bucket and dummy bucket should not be the same");
    }
    if (!dummyStruct.equals(LEGACY_FILESYSTEM)
        && !dummyStruct.equals(FILE_TREE)
        && !dummyStruct.equals(OBJECT_KV)) {
      throw new StorageInitializationException(
          "unsupported "
              + DUMMY_STRUCT
              + ": "
              + dummyStruct
              + ", expect LegacyFilesystem, FileTree or ObjectKV");
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
      if (prefix != null && isLegacyFilesystem()) {
        String dot = fileTreeParams.getOrDefault(ObjectTreeConfig.DOT, ESCAPED_DOT);
        prefix = IginxPaths.getLegacy(prefix, dot);
      }
    }
    this.embeddedPrefix = emptyToNull(prefix);
    fileTreeParams.remove("prefix");
    if (embeddedPrefix != null) {
      fileTreeParams.put("prefix", embeddedPrefix);
    }
    this.dummyConfig = ConfigFactory.parseMap(fileTreeParams, "s3 dummy config");
  }

  public static S3Config of(StorageEngineMeta meta) throws StorageInitializationException {
    return of(
        meta.getIp(), meta.getPort(), meta.getExtraParams(), meta.isHasData(), meta.isReadOnly());
  }

  public static S3Config of(
      String ip, int port, Map<String, String> params, boolean hasData, boolean readOnly)
      throws StorageInitializationException {
    try {
      return new S3Config(ip, port, params, hasData, readOnly);
    } catch (IOException | RuntimeException e) {
      throw new StorageInitializationException("invalid s3 params: " + e.getMessage(), e);
    }
  }

  /** Reads the address policy from bundled service capabilities, without connecting. */
  static HostPolicy hostPolicy(Map<String, String> params) throws IOException {
    String engine = emptyToNull(params.get(ENGINE));
    if (engine == null) {
      return HostPolicy.RESOLVE_TO_IP;
    }
    engine = engine.trim().toLowerCase(Locale.ROOT);
    if (!S3StrategyFactory.ENGINES.contains(engine)) {
      return HostPolicy.RESOLVE_TO_IP;
    }
    return HostPolicy.valueOf(loadMeta(engine).getProperty(HOST_POLICY));
  }

  private static Properties loadMeta(String engine) throws IOException {
    Properties meta = new Properties();
    loadResource(meta, DEFAULT_META_FILE);
    loadResource(meta, engine + META_FILE_SUFFIX);
    return meta;
  }

  private static void loadResource(Properties meta, String name) throws IOException {
    URL url = S3Config.class.getClassLoader().getResource(META_DIR + name);
    if (url == null) {
      throw new IOException("cannot find " + name);
    }
    try (InputStream in = url.openStream()) {
      meta.load(in);
    }
  }

  private static boolean parseBoolean(Properties meta, String key) {
    String value = meta.getProperty(key);
    if (value == null) {
      throw new IllegalArgumentException(key + " is not set in " + DEFAULT_META_FILE);
    }
    String trimmed = value.trim();
    if (trimmed.equalsIgnoreCase("true")) {
      return true;
    }
    if (trimmed.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException(key + " should be true or false, but was " + value);
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

  /** whether the param is given explicitly instead of taking the default value */
  public boolean hasParam(String key) {
    return emptyToNull(params.get(key)) != null;
  }

  /** lower case name of the service, one of {@link S3StrategyFactory#ENGINES} */
  public String getEngine() {
    return engine;
  }

  public boolean isVirtualStyle() {
    return virtualStyle;
  }

  public boolean isRegionRequired() {
    return regionRequired;
  }

  public boolean isAutoCreateBucket() {
    return autoCreateBucket;
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

  public boolean isLegacyFilesystem() {
    return LEGACY_FILESYSTEM.equals(dummyStruct);
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
