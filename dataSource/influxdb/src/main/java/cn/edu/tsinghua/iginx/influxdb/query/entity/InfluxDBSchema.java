/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
