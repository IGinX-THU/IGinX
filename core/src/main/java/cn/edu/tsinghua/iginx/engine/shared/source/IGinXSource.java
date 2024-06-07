package cn.edu.tsinghua.iginx.engine.shared.source;

import java.util.Objects;

public class IGinXSource extends AbstractSource {

  private final String ip;

  private final int port;

  public IGinXSource(String ip, int port) {
    super(SourceType.IGinX);
    this.ip = ip;
    this.port = port;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  @Override
  public Source copy() {
    return new IGinXSource(ip, port);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IGinXSource that = (IGinXSource) o;
    return port == that.port && Objects.equals(ip, that.ip);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, port);
  }

  @Override
  public String toString() {
    return "IGinXSource[" + "ip='" + ip + '\'' + ", port=" + port + ']';
  }
}
