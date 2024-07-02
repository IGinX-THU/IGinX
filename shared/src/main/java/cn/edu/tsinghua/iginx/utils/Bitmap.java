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

public class Bitmap {

  private final int size;

  private final byte[] bitmap;

  public Bitmap(int size) {
    this.size = size;
    this.bitmap = new byte[(int) Math.ceil(this.size * 1.0 / 8)];
  }

  public Bitmap(int size, byte[] bitmap) {
    this.size = size;
    this.bitmap = bitmap;
  }

  public void mark(int i) {
    if (i < 0 || i >= size) throw new IllegalArgumentException("unexpected index");
    int index = i / 8;
    int indexWithinByte = i % 8;
    bitmap[index] |= (1 << indexWithinByte);
  }

  public boolean get(int i) {
    if (i < 0 || i >= size) throw new IllegalArgumentException("unexpected index");
    int index = i / 8;
    int indexWithinByte = i % 8;
    return (bitmap[index] & (1 << indexWithinByte)) != 0;
  }

  public byte[] getBytes() {
    return this.bitmap;
  }

  public int getSize() {
    return size;
  }
}
