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
package cn.edu.tsinghua.iginx.integration.other;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderTest.class);

  private static final String DOWNLOAD_PATH = "../downloads";

  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  private static Session session;

  public FileLoaderTest() {}

  @BeforeClass
  public static void setUp() {
    try {
      session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("open session error.", e);
    }
  }

  @AfterClass
  public static void tearDown() {
    try {
      session.closeSession();
    } catch (SessionException e) {
      LOGGER.error("close session error.", e);
    }
  }

  /**
   * 每处理几块打一次日志，以每块10KB为标准
   *
   * @param fileSize 文件大小
   * @return 每次日志 / 块数
   */
  private int decideLogStep(long fileSize) {
    if (fileSize < 100 * 1024 * 1024) {
      // 100MB以内
      return 10;
    } else if (fileSize < 1024 * 1024 * 1024) {
      // 1GB以内
      return 100;
    } else {
      // 1GB以上
      return 1000;
    }
  }

  public void loadFile(String path) {
    if (path == null || path.trim().isEmpty()) {
      LOGGER.error("Invalid file path: {}", path);
      fail();
    }

    // 从文件路径生成数据列名：[最后一级目录名].[文件名]，文件名中的“-”和“.”都被替换为“_”
    Path filePath = Paths.get(path).toAbsolutePath();
    String fileName = filePath.getFileName().toString();
    Path parent = filePath.getParent();

    if (parent != null) {
      String parentDirName = parent.getFileName().toString();
      String columnName = (parentDirName + "." + fileName.replace(".", "_")).replace("-", "_");
      loadFile(path, columnName);
    } else {
      LOGGER.error("File {} can't be in root dir.", path);
      fail();
    }
  }

  public void loadFile(String path, String columnName) {
    loadFile(path, 10 * 1024, columnName);
  }

  /**
   * 加载文件数据到IGinX
   *
   * @param path 文件路径
   * @param chunkSize chunk大小，默认10KB
   * @param columnName 存入IGinX的列名
   */
  public void loadFile(String path, int chunkSize, String columnName) {
    try {
      FileInputStream inputStream = new FileInputStream(path);
      byte[] buffer = new byte[chunkSize];

      int index = 0;
      long fileSize = new File(path).length();
      int step = decideLogStep(fileSize);
      while ((inputStream.read(buffer)) != -1) {
        processChunk(buffer, columnName, index++, step);
      }
      inputStream.close();
    } catch (IOException e) {
      LOGGER.error("Read file {} error.", path, e);
      fail();
    } catch (SessionException e) {
      LOGGER.error("Insert data error.", e);
      fail();
    }
  }

  private void processChunk(byte[] chunk, String pathName, int chunkIndex, int step)
      throws SessionException {
    if (chunkIndex % step == 0) {
      LOGGER.info("Processing #{} chunk for {}", chunkIndex, pathName);
    }
    List<String> paths = new ArrayList<>(Collections.singletonList(pathName));

    long[] timestamps = new long[1];
    timestamps[0] = chunkIndex;

    Object[] values = new Object[1];
    values[0] = chunk;
    Object[] valueList = new Object[1];
    valueList[0] = values;

    List<DataType> dataTypeList = new ArrayList<>(Collections.singletonList(DataType.BINARY));

    session.insertRowRecords(paths, timestamps, valueList, dataTypeList, null);
  }

  @Test
  public void loadLargeImage() {
    String fileName = "large_img.jpg";
    loadFile(DOWNLOAD_PATH + "/" + fileName);
  }
}
