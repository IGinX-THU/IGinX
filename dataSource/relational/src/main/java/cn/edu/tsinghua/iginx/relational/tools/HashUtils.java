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
package cn.edu.tsinghua.iginx.relational.tools;

public class HashUtils {

  public static long toHash(String s) {
    char c[] = s.toCharArray();
    long hv = 0;
    long base = 131;
    for (int i = 0; i < c.length; i++) {
      hv = hv * base + (long) c[i]; // 利用自然数溢出，即超过 LONG_MAX 自动溢出，节省时间
    }
    if (hv < 0) {
      return -1 * hv;
    }
    return hv;
  }
}
