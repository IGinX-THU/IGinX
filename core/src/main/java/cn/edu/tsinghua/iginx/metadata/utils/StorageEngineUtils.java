package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.HAS_DATA;
import static cn.edu.tsinghua.iginx.conf.Constants.SCHEMA_PREFIX;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

public class StorageEngineUtils {

  public static boolean setSchemaPrefixInExtraParams(String type, Map<String, String> extraParams) {
    boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(HAS_DATA, "false"));
    boolean readOnly =
        Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
    if (type.equals("filesystem") || type.equals("parquet")) {
      if (hasData) {
        // 如果hasData为true，则参数中必须配置dummy_dir
        Pair<Boolean, String> dummyDirPair = getCanonicalPath(extraParams.get("dummy_dir"));
        if (!dummyDirPair.k) {
          return false;
        }
        String dummyDirPath = dummyDirPair.v;
        if (!readOnly) {
          // 如果hasData为true，且readOnly为false，则参数中必须配置dir，且不能与dummy_dir相同
          Pair<Boolean, String> dirPair = getCanonicalPath(extraParams.get("dir"));
          if (!dirPair.k) {
            return false;
          }
          String dirPath = dirPair.v;
          if (dummyDirPath.equals(dirPath)) {
            return false;
          }
        }
        String separator = System.getProperty("file.separator");
        // dummyDirPath是规范路径，一定不会以separator结尾
        String schemaPrefix = dummyDirPath.substring(dummyDirPath.lastIndexOf(separator) + 1);
        if (extraParams.containsKey(SCHEMA_PREFIX)) {
          extraParams.put(SCHEMA_PREFIX, extraParams.get(SCHEMA_PREFIX) + "." + schemaPrefix);
        } else {
          extraParams.put(SCHEMA_PREFIX, schemaPrefix);
        }
      } else {
        // 如果hasData为false，则参数中必须配置dir
        Pair<Boolean, String> dirPair = getCanonicalPath(extraParams.get("dir"));
        return dirPair.k;
      }
    }
    return true;
  }

  public static boolean isLocalParquet(StorageEngineMeta meta, String iginxIP, int iginxPort) {
    if (!meta.getStorageEngine().equals("parquet")) return false;
    String storageIP = meta.getIp();
    int storageIginxPort = Integer.parseInt(meta.getExtraParams().getOrDefault("iginx_port", "-1"));
    return ((isLocalIPAddress(storageIP) || storageIP.equals(iginxIP))
        && storageIginxPort == iginxPort);
  }

  private static boolean isLocalIPAddress(String ip) {
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

  private static Pair<Boolean, String> getCanonicalPath(String dir) {
    Pair<Boolean, String> invalidPair = new Pair<>(false, "");
    if (dir == null || dir.isEmpty()) { // 为空
      return invalidPair;
    }
    File file = new File(dir);
    if (file.exists() && !file.isDirectory()) { // 存在但不是目录
      return invalidPair;
    }
    if (!file.exists()) { // 不存在则尝试创建
      if (!file.mkdirs()) {
        return invalidPair;
      }
    }
    try {
      String canonicalPath = file.getCanonicalPath(); // 获取规范路径
      return new Pair<>(true, canonicalPath);
    } catch (IOException e) {
      return invalidPair;
    }
  }
}
