package org.apache.parquet.local.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.xerial.snappy.Snappy;

public class SnappyBytesInputDecompressor
    implements CompressionCodecFactory.BytesInputDecompressor {

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = Snappy.uncompress(ingoing);
    if (outgoing.length != uncompressedSize) {
      throw new IOException("Non-compressed data did not have matching uncompressed sizes.");
    }
    return BytesInput.from(outgoing);
  }

  @Override
  public void decompress(
      ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
      throws IOException {
    output.clear();
    int size = Snappy.uncompress(input, output);
    output.limit(size);
  }

  @Override
  public void release() {}
}
