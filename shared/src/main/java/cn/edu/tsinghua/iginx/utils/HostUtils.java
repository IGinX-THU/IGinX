package cn.edu.tsinghua.iginx.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

public class HostUtils {

  public static boolean isLocalHost(String host) {
    try {
      InetAddress address = InetAddress.getByName(host);
      if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
        return true;
      }
      return NetworkInterface.getByInetAddress(address) != null;
    } catch (UnknownHostException | SocketException e) {
      return false;
    }
  }

  // host name --> host address
  public static String convertHostNameToHostAddress(String hostName) {
    String hostAddress = hostName;
    try {
      InetAddress address = InetAddress.getByName(hostName);
      hostAddress = address.getHostAddress();
    } catch (UnknownHostException e) {
      return hostAddress;
    }
    return hostAddress;
  }
}
