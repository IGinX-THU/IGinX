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
package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.Objects;

public class BitmapView {

  private final Bitmap bitmap;

  private final int start;

  private final int end;

  public BitmapView(Bitmap bitmap, int start, int end) {
    Objects.requireNonNull(bitmap);
    this.bitmap = bitmap;
    if (end <= start) {
      throw new IllegalArgumentException("end index should greater than start index");
    }
    if (end > bitmap.getSize()) {
      throw new IllegalArgumentException("end index shouldn't greater than the size of bitmap");
    }
    this.start = start;
    this.end = end;
  }

  public boolean get(int i) {
    if (i < 0 || i >= end - start) {
      throw new IllegalArgumentException("unexpected index");
    }
    return bitmap.get(i + start);
  }

  public int getSize() {
    return end - start;
  }

  public Bitmap getBitmap() {
    return bitmap;
  }
}
