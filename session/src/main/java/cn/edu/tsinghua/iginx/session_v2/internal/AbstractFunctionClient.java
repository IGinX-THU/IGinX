package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.thrift.IService;

public abstract class AbstractFunctionClient {

  protected final IginXClientImpl iginXClient;

  protected final IService.Iface client;

  protected final long sessionId;

  public AbstractFunctionClient(IginXClientImpl iginXClient) {
    this.iginXClient = iginXClient;
    this.client = iginXClient.getClient();
    this.sessionId = iginXClient.getSessionId();
  }
}
