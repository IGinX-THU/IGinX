package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.util.Collections;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class TsfileConfig extends AbstractConfig {

  @Optional
  CompressionType compression = CompressionType.LZ4;

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }

  public static TsfileConfig of(Config config) {
    return of(config, TsfileConfig.class);
  }
}
