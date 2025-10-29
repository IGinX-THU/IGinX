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

import static cn.edu.tsinghua.iginx.client.constant.Constants.*;

import cn.edu.tsinghua.iginx.client.IginxClient;
import cn.edu.tsinghua.iginx.exception.SessionException;

public class NormalState implements InputState {

  @Override
  public String getPrompt(IginxClient client) {
    if (client.getValidSqlBuffer().toString().replaceAll("\\s+", " ").trim().isEmpty()) {
      return IGINX_CLI_PREFIX;
    } else {
      return IGINX_CLI_PREFIX_WAITING_INPUT;
    }
  }

  @Override
  public boolean handleInput(String command, IginxClient client) throws SessionException {
    StringBuilder buffer = client.getBuffer();
    StringBuilder validSqlBuffer = client.getValidSqlBuffer();

    int length = command.length(), i = 0;
    while (i < length) {
      char current = command.charAt(i);
      char next = (i + 1 < command.length()) ? command.charAt(i + 1) : '\0';

      // 处理行注释
      if (current == '-' && next == '-') {
        buffer.append(command.substring(i)).append(System.lineSeparator());
        return true;
      }

      // 处理块注释
      if (current == '/' && next == '*') {
        buffer.append(current).append(next);
        i += 2;
        client.setInputState(new BlockCommentState());
        // 将剩余字符交给新状态处理
        String remaining = (i < length) ? command.substring(i) : "";
        return client.getInputState().handleInput(remaining, client);
      }

      // 处理各种引号
      if (current == '\'' || current == '"' || current == '`') {
        buffer.append(current);
        validSqlBuffer.append(current);
        i++;
        client.setInputState(createQuoteState(current));
        // 将剩余字符交给新状态处理
        String remaining = (i < length) ? command.substring(i) : "";
        return client.getInputState().handleInput(remaining, client);
      }

      // 处理语句结束
      if (current == ';') {
        buffer.append(current);
        validSqlBuffer.append(current);
        if (client.handleInputStatement(buffer.toString()) == IginxClient.OperationResult.STOP) {
          return false;
        }
        i++;

        buffer.setLength(0);
        validSqlBuffer.setLength(0);
        continue;
      }

      // 普通字符
      buffer.append(current);
      validSqlBuffer.append(current);
      i++;
    }
    return true;
  }

  private InputState createQuoteState(char quote) {
    switch (quote) {
      case '\'':
        return new SingleQuoteState();
      case '"':
        return new DoubleQuoteState();
      case '`':
        return new BacktickQuoteState();
      default:
        throw new IllegalArgumentException("Unsupported quote character: " + quote);
    }
  }
}
