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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.vectordb.tools;

import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.support.NameSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusNameSystem;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameUtils {

  public static final NameSystem nameSystem = new MilvusNameSystem();

  public static NameSystem getNameSystem() {
    return nameSystem;
  }

  public static Pair<String, Integer> getPathAndVersion(String fullname) {
    String regex = "\\[\\[(\\d+)\\]\\]$";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(fullname);

    if (matcher.find()) {
      return new Pair<>(fullname.substring(0, matcher.start()), Integer.parseInt(matcher.group(1)));
    }

    return new Pair<>(fullname, 0);
  }

  public static String unescape(String input) {
    return nameSystem.unescape(input);
  }

  public static String escape(String input) {
    return nameSystem.escape(input);
  }
}
