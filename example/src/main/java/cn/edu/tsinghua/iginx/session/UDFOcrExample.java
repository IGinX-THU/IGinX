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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** 通过UDF，使用OCR对图像进行文字识别 */
public class UDFOcrExample {

  private static Session session;

  public static void main(String[] args) throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    // 首先将图像文件夹作为文件系统数据库加入IGinX
    Map<String, String> params = new HashMap<>();
    params.put("iginx_port", "6888");
    params.put("dummy_dir", "example/src/main/resources/OCR/files");
    params.put("is_read_only", "true");
    params.put("has_data", "true");
    params.put("schema_prefix", "OCR");
    session.addStorageEngine("127.0.0.1", 6668, StorageEngineType.filesystem, params);
    try {
      // 查询图片文件，由于.是IGinX的关键字，这里将source.png转写为source\png，接下来我们将用OCR识别source.png
      SessionExecuteSqlResult result = session.executeSql("SHOW COLUMNS;");
      result.print(false, "ms");
      // result:
      // Columns:
      // +--------------------+--------+
      // |                Path|DataType|
      // +--------------------+--------+
      // |OCR.files.source\png|  BINARY|
      // +--------------------+--------+
      // Total line number = 1

      // 注册UDF
      session.executeRegisterTask(
          "create function udsf \"ocrtext\" from \"UDSFOcr\" in \"example/src/main/resources/udsf_ocr.py\";");

      try {
        // 调用UDF，这里因为UDF需要返回两列，所以需要使用通配符*以及子查询；若只返回一列，则需要按照用户手册指导构造正确的返回列名
        result = session.executeSql("select ocrtext(*) from (select source\\png from OCR.files);");
        result.print(false, "ms");
      } finally {
        session.executeSql("drop function \"ocrtext\";");
      }
    } finally {
      session.removeStorageEngine(
          Collections.singletonList(new RemovedStorageEngineInfo("127.0.0.1", 6668, "OCR", "")),
          true);
    }

    session.closeSession();
  }
}
