package cn.edu.tsinghua.iginx.mongodb.tools;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Base16m {
  private static final int BASE_ASCII = 'a';
  private static final Charset CHARSET = StandardCharsets.UTF_8;

  public static String encode(String raw) {
    byte[] bytes = raw.getBytes(CHARSET);
    char[] code = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      code[2 * i] = (char) ((bytes[i] & 0xF) + BASE_ASCII);
      code[2 * i + 1] = (char) ((bytes[i] >>> 4) + BASE_ASCII);
    }
    return new String(code);
  }

  public static String decode(String code) {
    byte[] bytes = new byte[(code.length() + 1) / 2];
    for (int i = 0; i < code.length(); i++) {
      if (i % 2 == 0) {
        bytes[i / 2] = (byte) (code.charAt(i) - BASE_ASCII);
      } else {
        bytes[i / 2] |= (byte) ((code.charAt(i) - BASE_ASCII) << 4);
      }
    }
    return new String(bytes, CHARSET);
  }
}
