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

import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

// TODO: make it compatible with LZ4FrameOutputStream
public class FastestLz4CompressionCodec extends AbstractCompressionCodec {
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

    int uncompressedLength = Math.toIntExact(uncompressedBuffer.writerIndex());
    int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
    ArrowBuf compressedBuffer = allocator.buffer(Long.BYTES + maxCompressedLength);

    ByteBuffer nioUncompressedBuffer = uncompressedBuffer.nioBuffer(0, uncompressedLength);
    ByteBuffer nioCompressedBuffer = compressedBuffer.nioBuffer(Long.BYTES, maxCompressedLength);

    compressor.compress(nioUncompressedBuffer, nioCompressedBuffer);
    int compressedLength = nioCompressedBuffer.position();
    compressedBuffer.writerIndex(Long.BYTES + compressedLength);
    return compressedBuffer;
  }

  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    int uncompressedLength = Math.toIntExact(readUncompressedLength(compressedBuffer));

    int compressedLength = Math.toIntExact(compressedBuffer.writerIndex() - Long.BYTES);
    ArrowBuf uncompressedBuffer = allocator.buffer(uncompressedLength);

    ByteBuffer nioCompressedBuffer = compressedBuffer.nioBuffer(Long.BYTES, compressedLength);
    ByteBuffer nioUncompressedBuffer = uncompressedBuffer.nioBuffer(0, uncompressedLength);

    decompressor.decompress(nioCompressedBuffer, nioUncompressedBuffer);
    uncompressedBuffer.writerIndex(uncompressedLength);
    return uncompressedBuffer;
  }

  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }
}
