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
package cn.edu.tsinghua.iginx.utils;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class Escaper {
  private final char escapePrefix;
  private final Map<Character, Character> replacementMap;
  private final Map<Character, Character> reverseReplacementMap;

  public Escaper(char escapePrefix, Map<Character, Character> replacementMap) {
    this.escapePrefix = escapePrefix;
    this.replacementMap = new HashMap<>(replacementMap);
    this.reverseReplacementMap = new HashMap<>();
    for (Map.Entry<Character, Character> entry : replacementMap.entrySet()) {
      if (reverseReplacementMap.containsKey(entry.getValue())) {
        throw new IllegalArgumentException("replacementMap should not have duplicate values");
      }
      reverseReplacementMap.put(entry.getValue(), entry.getKey());
    }
    if (escape(escapePrefix) == null) {
      throw new IllegalArgumentException("replacementMap should contain escapePrefix");
    }
  }

  public char getEscapePrefix() {
    return escapePrefix;
  }

  public Character escape(char c) {
    return replacementMap.get(c);
  }

  public Character unescape(char c) {
    return reverseReplacementMap.get(c);
  }

  public String escape(CharSequence input) {
    StringBuilder sb = new StringBuilder();
    escape(input, sb);
    return sb.toString();
  }

  public void escape(CharSequence input, StringBuilder sb) {
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (replacementMap.containsKey(c)) {
        sb.append(escapePrefix);
        sb.append(replacementMap.get(c));
      } else {
        sb.append(c);
      }
    }
  }

  public String unescape(CharSequence input) throws ParseException {
    return unescape(input, 0, input.length());
  }

  public String unescape(CharSequence input, int start, int end) throws ParseException {
    StringBuilder sb = new StringBuilder();
    unescape(input, start, end, sb);
    return sb.toString();
  }

  public void unescape(CharSequence input, int start, int end, StringBuilder sb)
      throws ParseException {
    boolean escaped = false;
    for (int i = start; i < end; i++) {
      char c = input.charAt(i);
      if (escaped) {
        Character replacement = reverseReplacementMap.get(c);
        if (replacement == null) {
          throw new ParseException("Invalid escape character", i);
        }
        sb.append(replacement);
        escaped = false;
      } else if (c == escapePrefix) {
        escaped = true;
      } else {
        sb.append(c);
      }
    }
    if (escaped) {
      throw new ParseException("Missing escape character", input.length());
    }
  }
}
