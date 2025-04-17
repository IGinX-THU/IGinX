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
package cn.edu.tsinghua.iginx;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MyCounter {

  public static void main(String[] args) throws IOException {
    String path = "/Users/cauchy-ny/Downloads/benchmark-3.log";
    FileInputStream inputStream = new FileInputStream(path);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

    int time = 300000;
    int offset = time;
    int count = 0;

    String str;
    List<Integer> list = new ArrayList<>();
    while ((str = reader.readLine()) != null) {
      String[] arr = str.split(",");
      if (arr.length != 3) {
        continue;
      }
      int curTime = Integer.parseInt(arr[2]);
      if (curTime < time) {
        count++;
      } else {
        list.add(count);
        count = 1;
        time += offset;
      }
    }
    list.add(count);

    list.forEach(System.out::println);
  }
}
