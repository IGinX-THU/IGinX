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

public class RowDataView extends DataView {

  private final int[] biases;

  public RowDataView(
      RawData data, int startPathIndex, int endPathIndex, int startKeyIndex, int endKeyIndex) {
    super(data, startPathIndex, endPathIndex, startKeyIndex, endKeyIndex);
    this.biases = new int[this.endKeyIndex - this.startKeyIndex];
    for (int i = this.startKeyIndex; i < this.endKeyIndex; i++) {
      Bitmap bitmap = data.getBitmaps().get(i);
      for (int j = 0; j < this.startPathIndex; j++) {
        if (bitmap.get(j)) {
          biases[i - this.startKeyIndex]++;
        }
      }
    }
  }

  @Override
  public Object getValue(int index1, int index2) {
    checkTimeIndexRange(index1);
    Object[] tmp = (Object[]) data.getValuesList()[index1 + startKeyIndex];
    return tmp[biases[index1] + index2];
  }

  @Override
  public BitmapView getBitmapView(int index) {
    checkTimeIndexRange(index);
    return new BitmapView(
        data.getBitmaps().get(startKeyIndex + index), startPathIndex, endPathIndex);
  }
}
