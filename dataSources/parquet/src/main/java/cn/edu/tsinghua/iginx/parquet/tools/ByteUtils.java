/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
