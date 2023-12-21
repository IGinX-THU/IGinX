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
