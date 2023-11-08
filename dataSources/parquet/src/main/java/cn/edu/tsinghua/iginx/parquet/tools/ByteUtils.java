package cn.edu.tsinghua.iginx.parquet.tools;

public class ByteUtils {

  public static byte[] asBytes(int value) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (value >> 24);
    bytes[1] = (byte) ((value >> 16) & 0xFF);
    bytes[2] = (byte) ((value >> 8) & 0xFF);
    bytes[3] = (byte) ((value) & 0xFF);
    return bytes;
  }

  public static byte[] asBytes(long value) {
    byte[] bytes = new byte[8];
    bytes[0] = (byte) (value >> 56);
    bytes[1] = (byte) ((value >> 48) & 0xFF);
    bytes[2] = (byte) ((value >> 40) & 0xFF);
    bytes[3] = (byte) ((value >> 32) & 0xFF);
    bytes[4] = (byte) ((value >> 24) & 0xFF);
    bytes[5] = (byte) ((value >> 16) & 0xFF);
    bytes[6] = (byte) ((value >> 8) & 0xFF);
    bytes[7] = (byte) ((value) & 0xFF);
    return bytes;
  }

  public static byte[] asBytes(float value) {
    return asBytes(Float.floatToIntBits(value));
  }

  public static byte[] asBytes(double value) {
    return asBytes(Double.doubleToLongBits(value));
  }

  public static byte[] asBytes(boolean value) {
    return new byte[] {(byte) (value ? 0xFF : 0)};
  }

  public static byte[] concat(Iterable<byte[]> list) {
    int totalLength = 0;
    for (byte[] bytes : list) {
      totalLength += bytes.length;
    }
    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] bytes : list) {
      System.arraycopy(bytes, 0, result, offset, bytes.length);
      offset += bytes.length;
    }
    return result;
  }
}
