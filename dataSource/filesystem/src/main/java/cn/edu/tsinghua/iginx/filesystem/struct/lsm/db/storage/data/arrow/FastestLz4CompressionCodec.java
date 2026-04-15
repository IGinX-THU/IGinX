package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.nio.ByteBuffer;

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
