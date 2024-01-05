/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Count implements SetMappingFunction {

  public static final String COUNT = "count";

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(Count.class);

  private static final Count INSTANCE = new Count();

  private Count() {}

  public static Count getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.SetMapping;
  }

  @Override
  public String getIdentifier() {
    return COUNT;
  }

  @Override
  public Row transform(RowStream rows, FunctionParams params) throws Exception {
    List<String> pathParams = params.getPaths();
    if (pathParams == null || pathParams.size() != 1) {
      throw new IllegalArgumentException("unexpected param type for avg.");
    }

    String target = pathParams.get(0);
    Pattern pattern = Pattern.compile(StringUtils.reformatPath(target) + ".*");
    List<Field> targetFields = new ArrayList<>();
    List<Integer> indices = new ArrayList<>();
    for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
      Field field = rows.getHeader().getField(i);
      if (pattern.matcher(field.getFullName()).matches()) {
        String name = getIdentifier() + "(";
        String fullName = getIdentifier() + "(";
        if (params.isDistinct()) {
          name += "distinct ";
          fullName += "distinct ";
        }
        name += field.getName() + ")";
        fullName += field.getFullName() + ")";
        targetFields.add(new Field(name, fullName, DataType.LONG));
        indices.add(i);
      }
    }
    long[] counts = new long[targetFields.size()];
    while (rows.hasNext()) {
      Row row = rows.next();
      Object[] values = row.getValues();
      for (int i = 0; i < indices.size(); i++) {
        int index = indices.get(i);
        if (values[index] != null) {
          counts[i]++;
        }
      }
    }
    Object[] targetValues = new Object[targetFields.size()];
    for (int i = 0; i < counts.length; i++) {
      targetValues[i] = counts[i];
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
