/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;

public class FastestCompressionFactory implements CompressionCodec.Factory {

  public static final FastestCompressionFactory INSTANCE = new FastestCompressionFactory();

  private FastestCompressionFactory() {}

  public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
    switch (codecType) {
      case NO_COMPRESSION:
        return NoCompressionCodec.INSTANCE;
      case LZ4_FRAME:
        return new FastestLz4CompressionCodec();
      default:
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
    }
  }

  public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
    switch (codecType) {
      case NO_COMPRESSION:
        return NoCompressionCodec.INSTANCE;
      case LZ4_FRAME:
        return new FastestLz4CompressionCodec();
      default:
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel);
    }
  }
}
