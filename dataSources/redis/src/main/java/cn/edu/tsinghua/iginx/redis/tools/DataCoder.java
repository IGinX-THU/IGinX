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
package cn.edu.tsinghua.iginx.redis.tools;

public class DataCoder {

  public static byte[] encode(long number) {
    long encodeNumber = number + Long.MIN_VALUE;
    byte[] code = new byte[Long.BYTES];
    for (int i = 0; i < Long.BYTES; i++) {
      int shift = (Long.BYTES - 1 - i) * 8;
      code[i] = (byte) ((encodeNumber >>> shift) & 0xff);
    }
    return code;
  }

  public static byte[] encode(String str) {
    return redis.clients.jedis.util.SafeEncoder.encode(str);
  }

  public static long decodeToLong(byte[] code) throws IllegalArgumentException {
    if (code.length != Long.BYTES) {
      throw new IllegalArgumentException(
          String.format("`code.length`(%s) isn't equal to `Long.BYTES`.", code.length));
    }

    long encodeNumber = 0;
    for (int i = 0; i < Long.BYTES; i++) {
      int shift = (Long.BYTES - 1 - i) * 8;
      long tempNumber = ((long) (code[i]) & 0xff) << shift;
      encodeNumber = encodeNumber | tempNumber;
    }
    return encodeNumber - Long.MIN_VALUE;
  }

  public static String decodeToString(byte[] bytes) {
    return redis.clients.jedis.util.SafeEncoder.encode(bytes);
  }
}
