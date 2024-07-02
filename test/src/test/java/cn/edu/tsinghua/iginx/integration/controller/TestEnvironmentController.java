package cn.edu.tsinghua.iginx.integration.controller;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEnvironmentController {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestEnvironmentController.class);

  private static Session session;

  private static final String ADD_STORAGE_ENGINE =
      "ADD STORAGEENGINE (\"%s\", %s, \"%s\", \"%s\");";

  public void setTestTasks(List<String> taskList, String filePath) {
    try {
      File file = new File(filePath); // 文件路径
      FileWriter fileWriter = new FileWriter(file);
      LOGGER.info("test should run {}", taskList);
      for (String taskName : taskList) {
        fileWriter.write(taskName + "\n");
      }
      fileWriter.flush(); // 刷新数据，不刷新写入不进去
      fileWriter.close(); // 关闭流
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
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
