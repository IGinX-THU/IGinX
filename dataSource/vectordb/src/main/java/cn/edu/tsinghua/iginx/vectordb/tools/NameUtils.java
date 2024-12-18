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
package cn.edu.tsinghua.iginx.vectordb.tools;

import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.support.NameSystem;
import cn.edu.tsinghua.iginx.vectordb.support.impl.MilvusNameSystem;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameUtils {

  public static final NameSystem nameSystem = new MilvusNameSystem();

  public static NameSystem getNameSystem() {
    return nameSystem;
  }

  public static Pair<String, String> getPathAndVersion(String fullname) {
    String regex = "\\[\\[([a-zA-Z0-9]+)\\]\\]$";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(fullname);

    if (matcher.find()) {
      return new Pair<>(fullname.substring(0, matcher.start()), matcher.group(1));
    }

    return new Pair<>(fullname, "0");
  }

  public static String unescape(String input) {
    return nameSystem.unescape(input);
  }

  public static String escape(String input) throws UnsupportedEncodingException {
    return nameSystem.escape(input);
  }
}
