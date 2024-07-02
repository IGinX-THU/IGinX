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
package cn.edu.tsinghua.iginx.integration.mds;

import java.util.Random;

public class RandomUtils {

  private static final Random random = new Random();

  public static String randomString(int length) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int rand = random.nextInt(26 * 2 + 10);
      if (rand < 10) {
        builder.append(rand);
        continue;
      }
      rand -= 10;
      if (rand < 26) {
        builder.append((char) ('A' + rand));
        continue;
      }
      rand -= 26;
      builder.append((char) ('a' + rand));
    }
    return builder.toString();
  }

  public static int randomNumber(int lowBound, int upBound) {
    return random.nextInt(upBound - lowBound) + lowBound;
  }

  public static boolean randomTest(double chance) {
    return random.nextDouble() < chance;
  }
}
