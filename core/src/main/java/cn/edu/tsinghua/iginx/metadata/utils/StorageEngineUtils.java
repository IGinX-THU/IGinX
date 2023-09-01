package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.HAS_DATA;
import static cn.edu.tsinghua.iginx.conf.Constants.SCHEMA_PREFIX;

import cn.edu.tsinghua.iginx.conf.Constants;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class StorageEngineUtils {

  public static boolean setSchemaPrefixInExtraParams(String type, Map<String, String> extraParams) {
    boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(HAS_DATA, "false"));
    boolean readOnly =
        Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
    if (type.equals("filesystem")) {
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
}
