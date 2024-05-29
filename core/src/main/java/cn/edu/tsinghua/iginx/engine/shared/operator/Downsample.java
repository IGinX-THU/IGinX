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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Downsample extends AbstractUnaryOperator {

  private final long precision;

  private final long slideDistance;

  private final List<FunctionCall> functionCallList;

  private final KeyRange keyRange;

  public Downsample(
      Source source,
      long precision,
      long slideDistance,
      List<FunctionCall> functionCallList,
      KeyRange keyRange) {
    super(OperatorType.Downsample, source);
    if (precision <= 0) {
      throw new IllegalArgumentException("precision should be greater than zero");
    }
    if (slideDistance <= 0) {
      throw new IllegalArgumentException("slide distance should be greater than zero");
    }

    if (keyRange == null) {
      throw new IllegalArgumentException("timeRange shouldn't be null");
    }
    this.precision = precision;
    this.slideDistance = slideDistance;
    this.keyRange = keyRange;

    for (FunctionCall functionCall : functionCallList) {
      if (functionCall == null || functionCall.getFunction() == null) {
        throw new IllegalArgumentException("function shouldn't be null");
      }
      if (functionCall.getFunction().getMappingType() != MappingType.SetMapping) {
        throw new IllegalArgumentException("function should be set mapping function");
      }
    }
    this.functionCallList = functionCallList;
  }

  public long getPrecision() {
    return precision;
  }

  public long getSlideDistance() {
    return slideDistance;
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  public KeyRange getKeyRange() {
    return keyRange;
  }

  @Override
  public Operator copy() {
    return new Downsample(
        getSource().copy(),
        precision,
        slideDistance,
        new ArrayList<>(functionCallList),
        keyRange.copy());
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Downsample(
        source, precision, slideDistance, new ArrayList<>(functionCallList), keyRange.copy());
  }

  @Override
  public String getInfo() {
    StringBuilder sb =
        new StringBuilder(
            "Precision: "
                + precision
                + ", SlideDistance: "
                + slideDistance
                + ", TimeRange: "
                + keyRange.toString()
                + ", FuncList(Name, FunctionType): ");

    if (functionCallList == null || functionCallList.isEmpty()) {
      return sb.toString();
    }

    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }
    sb.append("MappingType: ");
    sb.append(functionCallList.get(0).getFunction().getMappingType());

    return sb.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Downsample that = (Downsample) object;
    return precision == that.precision
        && slideDistance == that.slideDistance
        && functionCallList.equals(that.functionCallList)
        && keyRange.equals(that.keyRange);
  }
}
