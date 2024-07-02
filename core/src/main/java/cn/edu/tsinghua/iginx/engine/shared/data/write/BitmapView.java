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
