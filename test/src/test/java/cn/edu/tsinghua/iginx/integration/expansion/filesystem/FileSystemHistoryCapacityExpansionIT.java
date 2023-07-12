package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.integration.expansion.filesystem.FileSystemHistoryDataGenerator.*;
import static cn.edu.tsinghua.iginx.integration.tool.DBType.filesystem;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryCapacityExpansionIT extends BaseCapacityExpansionIT {
  private static final Logger logger =
      LoggerFactory.getLogger(FileSystemHistoryCapacityExpansionIT.class);

  protected static Session session;
  protected static SessionPool sessionPool;
  protected String ENGINE_TYPE;

  public FileSystemHistoryCapacityExpansionIT() {
    super(filesystem, "username:root, password:root, sessionPoolSize:20", 4860, 4861, 4862);
  }

  @Override
  protected String addStorageEngine(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    try {
      StringBuilder statement = new StringBuilder();
      statement.append("ADD STORAGEENGINE (\"127.0.0.1\", ");
      statement.append(port);
      statement.append(", \"");
      statement.append(dbType.name());
      statement.append("\", \"");
      statement.append("has_data:");
      statement.append(hasData);
      statement.append(", is_read_only:");
      statement.append(isReadOnly);
      if (extraParams != null) {
        statement.append(", ");
        statement.append(extraParams);
      }
      if (dataPrefix != null) {
        statement.append(", data_prefix:");
        statement.append(dataPrefix);
      }
      if (schemaPrefix != null) {
        statement.append(", schema_prefix:");
        statement.append(schemaPrefix);
      }
      statement.append(", dir:");
      statement.append(getRootFromPort(port));

      statement.append("\");");

      logger.info("Execute Statement: \"{}\"", statement);
      session.executeSql(statement.toString());
    } catch (ExecutionException | SessionException e) {
      logger.error(
          "add storage engine {} port {} hasData {} isReadOnly {} dataPrefix {} schemaPrefix {} dir {} failure: {}",
          dbType.name(),
          port,
          hasData,
          isReadOnly,
          dataPrefix,
          schemaPrefix,
          getRootFromPort(port),
          e.getMessage());
    }
    return dataPrefix;
  }

  public String getRootFromPort(int port) {
    String root = null;
    if (port == oriPort) {
      root = root1;
    } else if (port == expPort) {
      root = root2;
    } else {
      root = root3;
    }
    return root;
  }
}
