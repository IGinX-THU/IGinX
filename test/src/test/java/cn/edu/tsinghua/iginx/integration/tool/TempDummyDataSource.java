package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TempDummyDataSource implements AutoCloseable {

  public static final String DEFAULT_IP = "127.0.0.1";
  public static final int DEFAULT_PORT = 16667;
  public static final String DEFAULT_SCHEMA_PREFIX = "";
  public static final String DEFAULT_PREFIX = "";

  private final Session session;
  private final String ip;
  private final int port;
  private final StorageEngineType type;
  private final String schemaPrefix;
  private final String dataPrefix;
  private final Map<String, String> extraParams;

  public TempDummyDataSource(
      Session session, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    this(
        session,
        DEFAULT_IP,
        DEFAULT_PORT,
        type,
        DEFAULT_SCHEMA_PREFIX,
        DEFAULT_PREFIX,
        extraParams);
  }

  public TempDummyDataSource(
      Session session,
      String ip,
      int port,
      StorageEngineType type,
      String schemaPrefix,
      String dataPrefix,
      Map<String, String> extraParams)
      throws SessionException {
    this.session = Objects.requireNonNull(session);
    this.ip = Objects.requireNonNull(ip);
    this.port = port;
    this.type = Objects.requireNonNull(type);
    this.schemaPrefix = Objects.requireNonNull(schemaPrefix);
    this.dataPrefix = Objects.requireNonNull(dataPrefix);
    this.extraParams = Objects.requireNonNull(extraParams);
    init();
  }

  private void init() throws SessionException {
    LinkedHashMap<String, String> params = new LinkedHashMap<>(extraParams);
    params.put("schema_prefix", schemaPrefix);
    params.put("data_prefix", dataPrefix);
    params.put("has_data", "true");
    params.put("is_read_only", "true");
    session.addStorageEngine(ip, port, type, params);
  }

  @Override
  public void close() throws SessionException {
    RemovedStorageEngineInfo info =
        new RemovedStorageEngineInfo(ip, port, schemaPrefix, dataPrefix);
    session.removeHistoryDataSource(Collections.singletonList(info));
  }
}
