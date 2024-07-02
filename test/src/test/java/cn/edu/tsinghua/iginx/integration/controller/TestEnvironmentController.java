/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
