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
