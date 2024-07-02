package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.utils.Bitmap;

public final class ColumnDataView extends DataView {

  private final int[] biases;

  public ColumnDataView(
      RawData data, int startPathIndex, int endPathIndex, int startKeyIndex, int endKeyIndex) {
    super(data, startPathIndex, endPathIndex, startKeyIndex, endKeyIndex);
    this.biases = new int[this.endPathIndex - this.startPathIndex];
    for (int i = this.startPathIndex; i < this.endPathIndex; i++) {
      Bitmap bitmap = data.getBitmaps().get(i);
      for (int j = 0; j < this.startKeyIndex; j++) {
        if (bitmap.get(j)) {
          biases[i - this.startPathIndex]++;
        }
      }
    }
  }

  @Override
  public Object getValue(int index1, int index2) { // 第一个维度为序列，第二个维度为数组中的偏移量
    checkPathIndexRange(index1);
    return ((Object[]) data.getValuesList()[index1 + startPathIndex])[biases[index1] + index2];
  }

  @Override
  public BitmapView getBitmapView(int index) { // 对于列数据来说，第一个维度为序列，所以要 checkPath
    checkPathIndexRange(index);
    return new BitmapView(
        data.getBitmaps().get(startPathIndex + index), startKeyIndex, endKeyIndex);
  }
}
