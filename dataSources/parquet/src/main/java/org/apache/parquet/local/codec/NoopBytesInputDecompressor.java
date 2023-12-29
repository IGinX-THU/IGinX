/*
 * Copyright 2023 IginX of Tsinghua University
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

package org.apache.parquet.local.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

public class NoopBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
  @Override
  public void decompress(
      ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
      throws IOException {
    if (compressedSize != uncompressedSize) {
      throw new IOException(
          "Non-compressed data did not have matching compressed and uncompressed sizes.");
    }
    output.clear();
    output.put((ByteBuffer) input.duplicate().position(0).limit(compressedSize));
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
    return bytes;
  }

  @Override
  public void release() {}
}
