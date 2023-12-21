/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.local;

import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.local.codec.NoopBytesInputCompressor;
import org.apache.parquet.local.codec.NoopBytesInputDecompressor;
import org.apache.parquet.local.codec.SnappyBytesInputCompressor;
import org.apache.parquet.local.codec.SnappyBytesInputDecompressor;

public class CodecFactory implements CompressionCodecFactory {

  private final Map<CompressionCodecName, BytesInputCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesInputDecompressor> decompressors = new HashMap<>();

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
    return createCompressor(codecName);
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
    return decompressors.computeIfAbsent(codecName, this::createDecompressor);
  }

  protected BytesInputCompressor createCompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return new NoopBytesInputCompressor();
      case SNAPPY:
        return new SnappyBytesInputCompressor();
      default:
        throw new IllegalArgumentException("Unimplemented codec: " + codecName);
    }
  }

  protected BytesInputDecompressor createDecompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return new NoopBytesInputDecompressor();
      case SNAPPY:
        return new SnappyBytesInputDecompressor();
      default:
        throw new IllegalArgumentException("Unimplemented codec: " + codecName);
    }
  }

  @Override
  public void release() {
    for (BytesInputCompressor compressor : compressors.values()) {
      compressor.release();
    }
    compressors.clear();
    for (BytesInputDecompressor decompressor : decompressors.values()) {
      decompressor.release();
    }
    decompressors.clear();
  }
}
