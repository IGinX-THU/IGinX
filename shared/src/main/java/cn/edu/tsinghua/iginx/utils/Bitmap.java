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
