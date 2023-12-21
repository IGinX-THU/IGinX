package org.apache.parquet.local.codec;

import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

public class SnappyBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    int maxOutputSize = Snappy.maxCompressedLength((int) bytes.size());
    byte[] outgoing = new byte[maxOutputSize];
    int compressedSize = Snappy.compress(bytes.toByteArray(), 0, (int) bytes.size(), outgoing, 0);
    return BytesInput.from(outgoing, 0, compressedSize);
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.SNAPPY;
  }

  @Override
  public void release() {}
}
