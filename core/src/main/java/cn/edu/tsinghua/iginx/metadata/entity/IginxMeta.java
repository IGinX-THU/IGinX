package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.pool.IginxInfo;
import java.util.Map;

public final class IginxMeta {

  /** iginx 的 id */
  private final long id;

  /** iginx 所在 ip */
  private final String ip;

  /** iginx 对外暴露的端口 */
  private final int port;

  /** iginx 其他控制参数 */
  private final Map<String, String> extraParams;

  public IginxMeta(long id, String ip, int port, Map<String, String> extraParams) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.extraParams = extraParams;
  }

  public long getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }

  public IginxInfo iginxMetaInfo() {
    return new IginxInfo.Builder()
        .host(ip)
        .port(port)
        .user(extraParams.getOrDefault("user", ""))
        .password(extraParams.getOrDefault("password", ""))
        .build();
  }
}
