package cn.edu.tsinghua.iginx.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

public class IPUtils {

  public static boolean isLocalIPAddress(String ip) {
    try {
      InetAddress address = InetAddress.getByName(ip);
      if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
        return true;
      }
      return NetworkInterface.getByInetAddress(address) != null;
    } catch (UnknownHostException | SocketException e) {
      return false;
    }
  }
}
