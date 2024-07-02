package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Map;

public final class Storage {

  private final String ip;

  private final int port;

  private final StorageEngineType type;

  private final Map<String, String> extraParams;

  public Storage(String ip, int port, StorageEngineType type, Map<String, String> extraParams) {
    this.ip = ip;
    this.port = port;
    this.type = type;
    this.extraParams = extraParams;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public StorageEngineType getType() {
    return type;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }
}
