package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import com.typesafe.config.*;
import com.typesafe.config.Optional;
import java.util.*;
import lombok.*;
import lombok.experimental.FieldNameConstants;

@Data
@With
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
public class FileTreeConfig extends AbstractConfig {

  @Optional String dot = "\\";

  @Optional String prefix = null;

  @Optional Map<String, Config> formats = Collections.emptyMap();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (validateNotNull(problems, Fields.dot, dot)) {
      if (dot.contains(".")) {
        problems.add(new InvalidFieldValidationProblem(Fields.dot, "dot cannot contain '.'"));
      }
    }
    return problems;
  }

  @SuppressWarnings("unchecked")
  public static FileTreeConfig of(Config config) {
    Config withoutFormats = config.withoutPath(Fields.formats);
    FileTreeConfig fileTreeConfig = of(withoutFormats, FileTreeConfig.class);

    if (config.hasPath(Fields.formats)) {
      ConfigValue value = config.getValue(Fields.formats);
      if (value.valueType() == ConfigValueType.OBJECT) {
        Map<String, Object> formatsRawConfig = (Map<String, Object>) value.unwrapped();
        Map<String, Config> formats = new HashMap<>();
        for (Map.Entry<String, Object> entry : formatsRawConfig.entrySet()) {
          if (entry.getValue() instanceof Map) {
            formats.put(
                entry.getKey(), ConfigFactory.parseMap((Map<String, Object>) entry.getValue()));
          }
        }
        fileTreeConfig.setFormats(formats);
      }
    }

    return fileTreeConfig;
  }
}
