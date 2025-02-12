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
package cn.edu.tsinghua.iginx.influxdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class SchemaTransformer {
  public static Pair<String, String> processPatternForQuery(
      String pattern, TagFilter tagFilter) { // 返回的是 bucket_name, query 的信息
    String[] parts = pattern.split("\\.", 3);
    int index = 0;
    String bucketName = parts[index++];
    if (parts.length == 1) {
      return new Pair<>(bucketName, "true");
    }
    StringBuilder queryBuilder = new StringBuilder("(");
    if (parts.length > 2) {
      String measurementName = parts[index++];
      if (!measurementName.equals("*")) {
        queryBuilder.append(String.format("r._measurement ==\"%s\" and ", measurementName));
      }
    }

    // 接着处理 field
    String field = parts[index];
    queryBuilder.append("r._field =~ /").append(InfluxDBSchema.transformField(field)).append("/");

    queryBuilder.append(")");
    return new Pair<>(bucketName, queryBuilder.toString());
  }
}
