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
package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;

public class FunctionCall {

  private final Function function;

  private final FunctionParams params;

  public FunctionCall(Function function, FunctionParams params) {
    this.function = function;
    this.params = params;
  }

  public Function getFunction() {
    return function;
  }

  public FunctionParams getParams() {
    return params;
  }

  public FunctionCall copy() {
    return new FunctionCall(function, params.copy());
  }

  @Override
  public String toString() {
    return String.format(
        "{Name: %s, FuncType: %s, MappingType: %s}",
        function.getIdentifier(), function.getFunctionType(), function.getMappingType());
  }

  private String getFuncName() {
    if (function instanceof ArithmeticExpr) {
      return params.getExpr().getColumnName();
    } else if (function.getFunctionType() == FunctionType.UDF) {
      if (function instanceof UDAF) {
        return ((UDAF) function).getFunctionName();
      } else if (function instanceof UDTF) {
        return ((UDTF) function).getFunctionName();
      } else if (function instanceof UDSF) {
        return ((UDSF) function).getFunctionName();
      }
    } else {
      return function.getIdentifier();
    }
    return null;
  }

  public String getFunctionStr() {
    if (function instanceof ArithmeticExpr) {
      return params.getExpr().getColumnName();
    }

    return String.format(
        "%s(%s%s)",
        getFuncName(),
        params.isDistinct() ? "distinct " : "",
        String.join(", ", params.getPaths()));
  }

  public String getNameAndFuncTypeStr() {
    return String.format(
        "(%s, %s)", FunctionUtils.getFunctionName(function), function.getFunctionType());
  }
}
