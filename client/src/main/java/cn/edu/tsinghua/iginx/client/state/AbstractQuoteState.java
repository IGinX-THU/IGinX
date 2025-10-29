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

import cn.edu.tsinghua.iginx.client.IginxClient;
import cn.edu.tsinghua.iginx.exception.SessionException;

public abstract class AbstractQuoteState implements InputState {
  private final char quote;

  public AbstractQuoteState(char quote) {
    this.quote = quote;
  }

  @Override
  public boolean handleInput(String command, IginxClient client) throws SessionException {
    StringBuilder buffer = client.getBuffer();
    StringBuilder validSqlBuffer = client.getValidSqlBuffer();

    int length = command.length(), i = 0;
    while (i < length) {
      char current = command.charAt(i);
      char last = (i - 1 >= 0) ? command.charAt(i - 1) : '\0';
      buffer.append(current);
      validSqlBuffer.append(current);

      // 检查是否遇到结束的引号（考虑转义情况）
      if (current == quote && last != '\\') {
        client.setInputState(new NormalState());
        // 将剩余字符交给新状态处理
        String remaining = (i + 1 < length) ? command.substring(i + 1) : "";
        return client.getInputState().handleInput(remaining, client);
      }

      i++;
    }
    return true;
  }
}
