package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import com.typesafe.config.Optional;

import java.util.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class FileTreeConfig extends AbstractConfig {

  @Optional String dot = "\\";

  @Optional boolean filenameAsPrefix = true;

  @Optional Map<String, Config> formats = Collections.emptyMap();

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }

  public static FileTreeConfig of(Config config) {
    Config withoutFormats = config.withoutPath(Fields.formats);
    FileTreeConfig fileTreeConfig = of(withoutFormats, FileTreeConfig.class);

    if (config.hasPath(Fields.formats)) {
      ConfigValue value = config.getValue(Fields.formats);
      if (value.valueType() == ConfigValueType.OBJECT) {
        Config formatsRawConfig = (Config) value.unwrapped();
        Map<String, Config> formats = parseFormats(formatsRawConfig);
        fileTreeConfig.setFormats(formats);
      }
    }

    return fileTreeConfig;
  }

  private static Map<String, Config> parseFormats(Config config) {
    Map<String, Config> formats = new HashMap<>();
    for (String key : config.root().keySet()) {
      ConfigValue value = config.getValue(key);
      if (value.valueType() == ConfigValueType.OBJECT) {
        formats.put(key, (Config) value.unwrapped());
      }
    }
    return formats;
  }
}
