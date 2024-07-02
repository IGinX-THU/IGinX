package cn.edu.tsinghua.iginx.influxdb.query.entity;

import java.util.Collections;
import java.util.Map;

public class InfluxDBSchema {

  public static final String TAG = "t";

  private final String measurement;

  private final String field;

  private final Map<String, String> tags;

  public InfluxDBSchema(String path, Map<String, String> tags) {
    int index = path.indexOf(".");
    this.measurement = path.substring(0, index);
    this.field = path.substring(index + 1);

    if (tags == null) {
      this.tags = Collections.emptyMap();
    } else {
      this.tags = tags;
    }
  }

  public InfluxDBSchema(String path) {
    this(path, null);
  }

  public String getMeasurement() {
    return measurement;
  }

  public String getField() {
    return field;
  }

  /**
   * 获取用作引号中string的field字段，这里需要将\转义为\\，其他无需转义
   *
   * @return 转义后的field字段
   */
  public String getFieldString() {
    return field.replace("\\", "\\\\");
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public static String transformField(String field) {
    String[] parts = field.split("\\.");
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (i != 0) {
        builder.append(".");
      }
      if (parts[i].equals("*")) {
        builder.append(".+");
      } else {
        builder.append(parts[i]);
      }
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return "InfluxDBSchema{"
        + "measurement='"
        + measurement
        + '\''
        + ", field='"
        + field
        + '\''
        + ", tags="
        + tags
        + '}';
  }
}
