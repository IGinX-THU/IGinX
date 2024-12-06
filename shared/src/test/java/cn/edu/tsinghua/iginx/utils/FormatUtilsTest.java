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
package cn.edu.tsinghua.iginx.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class FormatUtilsTest {

  @Test
  void formatResultChineseAlignment() {
    // Test Chinese alignment
    String result =
        FormatUtils.formatResult(
            Arrays.asList(
                Arrays.asList("时间", "数值"),
                Arrays.asList("2021-01-01T00:00:00.000", "1"),
                Arrays.asList("2021-01-02T00:00:00.000", "2"),
                Arrays.asList("2021-01-03T00:00:00.000", "3")));
    System.out.println(result);
    assertEquals(
        "+-----------------------+----+\n"
            + "|                   时间|数值|\n"
            + "+-----------------------+----+\n"
            + "|2021-01-01T00:00:00.000|   1|\n"
            + "|2021-01-02T00:00:00.000|   2|\n"
            + "|2021-01-03T00:00:00.000|   3|\n"
            + "+-----------------------+----+\n",
        result);
  }
}
