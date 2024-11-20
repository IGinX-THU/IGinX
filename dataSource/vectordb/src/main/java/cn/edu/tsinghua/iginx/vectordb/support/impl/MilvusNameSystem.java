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
package cn.edu.tsinghua.iginx.vectordb.support.impl;

import cn.edu.tsinghua.iginx.vectordb.support.NameSystem;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class MilvusNameSystem implements NameSystem {
  private static final char ESCAPE_CHAR = '_';
  private static final String ENCODE = "UTF-8";
  private static final Map<Character, String> escapeMap = new HashMap<>();
  private static final Map<String, Character> unescapeMap = new HashMap<>();

  static {
    escapeMap.put('_', "__");
    escapeMap.put('.', "_1");
    escapeMap.put(':', "_2");
    escapeMap.put(',', "_3");
    escapeMap.put('[', "_4");
    escapeMap.put(']', "_5");
    escapeMap.put('-', "_6");
    escapeMap.put('=', "_7");
    escapeMap.put('%', "_8");
    escapeMap.put('+', "_9");

    for (Map.Entry<Character, String> entry : escapeMap.entrySet()) {
      unescapeMap.put(entry.getValue(), entry.getKey());
    }
  }

  /**
   * 对字符串进行转义
   *
   * @param input 输入字符串
   * @return 转义后的字符串
   * @throws IllegalArgumentException 如果遇到不允许的字符
   */
  public String escape(String input) throws IllegalArgumentException, UnsupportedEncodingException {
    if (input.matches("^[_0-9].*")) {
      input = "_" + input;
    }
    String encoded = URLEncoder.encode(input, ENCODE);
    StringBuilder sb = new StringBuilder();
    for (char c : encoded.toCharArray()) {
      if (Character.isLetterOrDigit(c)) {
        sb.append(c);
      } else if (escapeMap.containsKey(c)) {
        sb.append(escapeMap.get(c));
      } else {
        throw new IllegalArgumentException("Invalid character: " + c);
      }
    }
    return sb.toString();
  }

  /**
   * 对字符串进行反转义
   *
   * @param input 转义后的字符串
   * @return 原始字符串
   */
  public String unescape(String input) {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    while (i < input.length()) {
      if (input.charAt(i) == ESCAPE_CHAR && i + 1 < input.length()) { // 最长的转义序列是_7，长度为2
        String escaped = input.substring(i, i + 2); // 取出可能的转义序列
        if (unescapeMap.containsKey(escaped)) {
          sb.append(unescapeMap.get(escaped));
          i += escaped.length();
        } else {
          sb.append(input.charAt(i++));
        }
      } else {
        sb.append(input.charAt(i++));
      }
    }
    try {
      String output = URLDecoder.decode(sb.toString(), ENCODE);
      if (output.startsWith("_")) {
        return output.substring(1);
      }
      return output;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
