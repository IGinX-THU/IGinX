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
