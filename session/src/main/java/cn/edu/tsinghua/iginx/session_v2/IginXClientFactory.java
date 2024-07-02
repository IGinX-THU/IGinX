package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.internal.IginXClientImpl;

public final class IginXClientFactory {

  private IginXClientFactory() {}

  public static IginXClient create() {
    return create("127.0.0.1", 6888);
  }

  public static IginXClient create(String url) {
    IginXClientOptions options = IginXClientOptions.builder().url(url).build();
    return create(options);
  }

  public static IginXClient create(String host, int port) {
    IginXClientOptions options = IginXClientOptions.builder().host(host).port(port).build();
    return create(options);
  }

  public static IginXClient create(String url, String username, String password) {
    IginXClientOptions options =
        IginXClientOptions.builder().url(url).username(username).password(password).build();
    return create(options);
  }

  public static IginXClient create(String host, int port, String username, String password) {
    IginXClientOptions options =
        IginXClientOptions.builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .build();
    return create(options);
  }

  public static IginXClient create(IginXClientOptions options) {
    Arguments.checkNotNull(options, "IginXClientOptions");
    return new IginXClientImpl(options);
  }
}
