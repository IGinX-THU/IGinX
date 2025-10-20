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
package cn.edu.tsinghua.iginx.integration.client;

import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.integration.tool.ClientLauncher;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecognizeDelimiterIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecognizeDelimiterIT.class);
  ClientLauncher client;

  @Before
  public void setUp() {
    client = new ClientLauncher();
  }

  @After
  public void tearDown() {
    client.close();
  }

  @Test
  public void recognizeDelimiter() {
    // 检测分号在注释或引号里能否被正确识别
    client.readLine("select * /*comment;*/ from * where a=\"`;'\"; -- com;ment");
    assertTrue(
        client.getResult().contains("ResultSets") && !client.getResult().contains("Parse Error"));

    // 模拟用户逐行输入
    client.readLine("show -- comment");
    client.readLine("/*");
    client.readLine("comment;");
    client.readLine("*/cluster");
    client.readLine("info;");
    assertTrue(
        client.getResult().contains("IginX infos") && !client.getResult().contains("Parse Error"));

    // 检测有语句残留在 buffer 中
    client.readLine("select *");
    client.readLine("from (show columns); select a.*");
    assertTrue(
        client.getResult().contains("ResultSets") && !client.getResult().contains("Parse Error"));
    client.readLine("from *;");
    assertTrue(
        client.getResult().contains("ResultSets") && !client.getResult().contains("Parse Error"));
  }
}
