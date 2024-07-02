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

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EscaperTest {

  @Test
  public void escape() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put('$', '!');
    Escaper escaper = new Escaper('\\', replacementMap);

    assertEquals("hello\\!world", escaper.escape("hello$world"));
    assertEquals("hello\\\\world", escaper.escape("hello\\world"));
  }

  @Test
  public void unescape() throws ParseException {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put('$', '!');
    Escaper escaper = new Escaper('\\', replacementMap);

    assertEquals("hello$world", escaper.unescape("hello\\!world"));
    assertEquals("hello\\world", escaper.unescape("hello\\\\world"));
  }
}
