package cn.edu.tsinghua.iginx.pool;

public class IginxInfo {
  /** iginx 的 host */
  private final String host;

  /** iginx 对外暴露的端口 */
  private final int port;

  /** iginx 用户名 */
  private final String user;

  /** iginx 的密码 */
  private final String password;

  IginxInfo(String host, int port, String user, String password) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public static class Builder {
    private String host;
    private int port;
    private String user;
    private String password;

    public IginxInfo.Builder host(String host) {
      this.host = host;
      return this;
    }

    public IginxInfo.Builder port(int port) {
      this.port = port;
      return this;
    }

    public IginxInfo.Builder user(String user) {
      this.user = user;
      return this;
    }

    public IginxInfo.Builder password(String password) {
      this.password = password;
      return this;
    }

    public IginxInfo build() {
      return new IginxInfo(host, port, user, password);
    }
  }
}
