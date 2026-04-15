package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.github.luben.zstd.Zstd;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.util.Collections;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ArrowConfig extends AbstractConfig {

  @Optional
  Integer compressionLevel = Zstd.defaultCompressionLevel();

  @Optional
  CompressionUtil.CodecType compression = CompressionUtil.CodecType.LZ4_FRAME;

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }

  public static ArrowConfig of(Config config) {
    return of(config, ArrowConfig.class);
  }
}
