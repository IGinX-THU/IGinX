package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.HAS_DATA;
import static cn.edu.tsinghua.iginx.conf.Constants.SCHEMA_PREFIX;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
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
        String dummyDir = extraParams.get("dummy_dir");
        if (dummyDir == null || dummyDir.isEmpty()) {
          return false;
        }
        if (!readOnly) {
          // 如果hasData为true，且readOnly为false，则参数中必须配置dir，且不能与dummy_dir相同
          String dir = extraParams.get("dir");
          if (dir == null || dir.isEmpty()) {
            return false;
          }
          try {
            String dummyDirPath = new File(dummyDir).getCanonicalPath();
            String dirPath = new File(dir).getCanonicalPath();
            if (dummyDirPath.equals(dirPath)) {
              return false;
            }
          } catch (IOException e) {
            return false;
          }
        }
        String schemaPrefix;
        String separator = System.getProperty("file.separator");
        if (dummyDir.endsWith(separator)) {
          String tempSchemaPrefix = dummyDir.substring(0, dummyDir.lastIndexOf(separator));
          schemaPrefix = tempSchemaPrefix.substring(tempSchemaPrefix.lastIndexOf(separator) + 1);
        } else {
          schemaPrefix = dummyDir.substring(dummyDir.lastIndexOf(separator) + 1);
        }
        if (extraParams.containsKey(SCHEMA_PREFIX)) {
          extraParams.put(SCHEMA_PREFIX, extraParams.get(SCHEMA_PREFIX) + "." + schemaPrefix);
        } else {
          extraParams.put(SCHEMA_PREFIX, schemaPrefix);
        }
      } else {
        // 如果hasData为false，则参数中必须配置dir
        String dir = extraParams.get("dir");
        if (dir == null || dir.isEmpty()) {
          return false;
        }
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
}
