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
      NetworkInterface ni = NetworkInterface.getByInetAddress(address);
      if (ni != null && ni.isVirtual()) {
        return true;
      }
      InetAddress local = InetAddress.getLocalHost();
      return local.equals(address);
    } catch (UnknownHostException | SocketException e) {
      return false;
    }
  }
}
