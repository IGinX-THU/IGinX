package cn.edu.tsinghua.iginx.integration.controller;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEnvironmentController {

  private static final Logger logger = LoggerFactory.getLogger(TestEnvironmentController.class);

  private static Session session;

  private static final String ADD_STORAGE_ENGINE =
      "ADD STORAGEENGINE (\"%s\", %s, \"%s\", \"%s\");";

  public TestEnvironmentController() {
    session = new Session("127.0.0.1", 6888, "root", "root");
    try {
      session.openSession();
    } catch (SessionException e) {
      logger.error(e.getMessage());
    }
  }

  public void addStorageEngine(StorageEngineMeta meta) throws Exception {
    session.executeSql(toCmd(meta));
  }

  public void setTestTasks(List<String> taskList, String filePath) {
    try {
      File file = new File(filePath); // 文件路径
      FileWriter fileWriter = new FileWriter(file);
      logger.info("test should run {}", taskList);
      for (String taskName : taskList) {
        fileWriter.write(taskName + "\n");
      }
      fileWriter.flush(); // 刷新数据，不刷新写入不进去
      fileWriter.close(); // 关闭流
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private String toCmd(StorageEngineMeta meta) {
    StringBuilder extraArg = new StringBuilder();
    for (Map.Entry<String, String> entry : meta.getExtraParams().entrySet()) {
      extraArg.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
    }
    extraArg.deleteCharAt(extraArg.length() - 1);
    return String.format(
        ADD_STORAGE_ENGINE, meta.getIp(), meta.getPort(), meta.getStorageEngine(), extraArg);
  }
}
