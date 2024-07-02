package cn.edu.tsinghua.iginx.mqtt;

import io.moquette.broker.security.IAuthenticator;

public class BrokerAuthenticator implements IAuthenticator {

  @Override
  public boolean checkValid(String client, String username, byte[] password) {
    return true;
  }
}
