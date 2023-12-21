package org.apache.parquet.local.codec;

import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class NoopBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {
  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    return bytes;
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.UNCOMPRESSED;
  }

  @Override
  public void release() {}
}
