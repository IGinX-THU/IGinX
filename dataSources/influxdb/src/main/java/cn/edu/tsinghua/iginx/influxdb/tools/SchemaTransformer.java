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
package cn.edu.tsinghua.iginx.influxdb.tools;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaTransformer {

  public static Field toField(String bucket, FluxTable table) {
    FluxRecord record = table.getRecords().get(0);
    String measurement = record.getMeasurement();
    String field = record.getField();
    List<FluxColumn> columns = table.getColumns();
    columns = columns.subList(8, columns.size());
    List<Pair<String, String>> tagKVs = new ArrayList<>();
    for (FluxColumn column : columns) {
      String tagK = column.getLabel();
      String tagV = (String) record.getValueByKey(tagK);
      tagKVs.add(new Pair<>(tagK, tagV));
    }
    tagKVs.sort(Comparator.comparing(o -> o.k));
    DataType dataType =
        fromInfluxDB(
            table.getColumns().stream()
                .filter(x -> x.getLabel().equals("_value"))
                .collect(Collectors.toList())
                .get(0)
                .getDataType());

    StringBuilder pathBuilder = new StringBuilder();
    pathBuilder.append(bucket);
    pathBuilder.append('.');
    pathBuilder.append(measurement);
    pathBuilder.append('.');
    pathBuilder.append(field);
    Map<String, String> tags = new HashMap<>();
    for (Pair<String, String> tagKV : tagKVs) {
      tags.put(tagKV.k, tagKV.v);
    }
    return new Field(pathBuilder.toString(), dataType, tags);
  }

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
