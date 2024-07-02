package cn.edu.tsinghua.iginx.session_v2;

public final class IginXClientOptions {

  private static final String DEFAULT_USERNAME = "root";

  private static final String DEFAULT_PASSWORD = "root";

  private final String host;

  private final int port;

  private final String username;

  private final String password;

  private IginXClientOptions(IginXClientOptions.Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.username = builder.username;
    this.password = builder.password;
  }

  public static IginXClientOptions.Builder builder() {
    return new IginXClientOptions.Builder();
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public static class Builder {

    private String host;

    private int port;

    private String username;

    private String password;

    private Builder() {}

    public IginXClientOptions.Builder url(String url) {
      Arguments.checkUrl(url, "url");
      // TODO: 将 url 拆分成 host + port
      return this;
    }

    public IginXClientOptions.Builder host(String host) {
      Arguments.checkNonEmpty(host, "host");
      this.host = host;
      return this;
    }

    public IginXClientOptions.Builder port(int port) {
      this.port = port;
      return this;
    }

    public IginXClientOptions.Builder authenticate(String username, String password) {
      Arguments.checkNonEmpty(username, "username");
      Arguments.checkNonEmpty(password, "password");
      this.username = username;
      this.password = password;
      return this;
    }

    public IginXClientOptions.Builder username(String username) {
      Arguments.checkNonEmpty(username, "username");
      this.username = username;
      return this;
    }

    public IginXClientOptions.Builder password(String password) {
      Arguments.checkNonEmpty(password, "password");
      this.password = password;
      return this;
    }

    public IginXClientOptions build() {
      if (this.host == null || this.port == 0) {
        throw new IllegalStateException("the host and port to connect to Iginx has to be defined.");
      }
      if (this.username == null) {
        this.username = DEFAULT_USERNAME;
      }
      if (this.password == null) {
        this.password = DEFAULT_PASSWORD;
      }
      return new IginXClientOptions(this);
    }
  }
}
