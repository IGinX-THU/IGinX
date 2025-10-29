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
package cn.edu.tsinghua.iginx.client.state;

import static cn.edu.tsinghua.iginx.client.constant.Constants.IGINX_CLI_PREFIX_BLOCK_COMMENT;

import cn.edu.tsinghua.iginx.client.IginxClient;
import cn.edu.tsinghua.iginx.exception.SessionException;

public class BlockCommentState implements InputState {

  @Override
  public String getPrompt(IginxClient client) {
    return IGINX_CLI_PREFIX_BLOCK_COMMENT;
  }

  @Override
  public boolean handleInput(String command, IginxClient client) throws SessionException {
    StringBuilder buffer = client.getBuffer();

    int length = command.length(), i = 0;
    while (i < length) {
      char current = command.charAt(i);
      char next = (i + 1 < length) ? command.charAt(i + 1) : '\0';

      buffer.append(current);

      if (current == '/' && next == '*') { // 处理嵌套的块注释开始
        buffer.append(next);
        i += 2;
        client.incrementBlockCommentDepth();
      } else if (current == '*' && next == '/') { // 处理块注释结束
        buffer.append(next);
        i += 2;
        client.decrementBlockCommentDepth();
        // 如果所有嵌套的块注释都已结束，则返回到正常状态
        if (client.getBlockCommentDepth() == 0) {
          client.setInputState(new NormalState());
          // 将剩余字符交给新状态处理
          String remaining = (i < length) ? command.substring(i) : "";
          return client.getInputState().handleInput(remaining, client);
        }
      }
    }
    return true;
  }
}
