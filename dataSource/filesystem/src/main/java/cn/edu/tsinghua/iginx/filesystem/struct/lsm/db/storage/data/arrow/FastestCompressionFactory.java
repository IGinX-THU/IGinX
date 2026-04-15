package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;

public class FastestCompressionFactory implements CompressionCodec.Factory {

  public static final FastestCompressionFactory INSTANCE = new FastestCompressionFactory();

  private FastestCompressionFactory() {
  }

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
