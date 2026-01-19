/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
    StringBuilder options = new StringBuilder();
    List<String> optionList = new ArrayList<>();
    for (Map.Entry<String, String> entry : meta.getExtraParams().entrySet()) {
      optionList.add(entry.getKey() + " '" + entry.getValue() + "'");
    }
    options.append("OPTIONS (").append(String.join(", ", optionList)).append(")");
    return String.format(
        "ADD STORAGEENGINE (\"%s\", %s, \"%s\", %s);",
        meta.getIp(), meta.getPort(), meta.getStorageEngine(), options.toString());
  }
}
