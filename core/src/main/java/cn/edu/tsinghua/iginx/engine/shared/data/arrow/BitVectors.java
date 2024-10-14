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
package cn.edu.tsinghua.iginx.engine.shared.data.arrow;

import org.apache.arrow.memory.ArrowBuf;

public class BitVectors {

  private BitVectors() {}

  public static void and(
      ArrowBuf retValidityBuffer,
      ArrowBuf firstValidityBuffer,
      ArrowBuf secondValidityBuffer,
      int byteCount) {
    // set by long
    for (long i = 0; i < byteCount / Long.BYTES; i++) {
      long first = firstValidityBuffer.getLong(i * Long.BYTES);
      long second = secondValidityBuffer.getLong(i * Long.BYTES);
      retValidityBuffer.setLong(i * Long.BYTES, first & second);
    }
    // set by byte
    for (int i = (byteCount / Long.BYTES) * Long.BYTES; i < byteCount; i++) {
      byte first = firstValidityBuffer.getByte(i);
      byte second = secondValidityBuffer.getByte(i);
      retValidityBuffer.setByte(i, first & second);
    }
  }
}
