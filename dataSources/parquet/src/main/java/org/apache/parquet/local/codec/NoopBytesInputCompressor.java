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
