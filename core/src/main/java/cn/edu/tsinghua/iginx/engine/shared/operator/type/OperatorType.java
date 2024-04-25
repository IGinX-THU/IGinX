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
package cn.edu.tsinghua.iginx.engine.shared.operator.type;

public enum OperatorType {

  // Exception[0,9]
  Unknown(0),

  // MultipleOperator[10,19]
  CombineNonQuery(10),
  Folded,

  // isGlobalOperator[20,29]
  ShowColumns(20),
  Migration,

  // BinaryOperator[30,49]
  Join(30),
  PathUnion,
  // SetOperator[35, 39)
  Union(35),
  Except,
  Intersect,
  // JoinOperator[40,49]
  InnerJoin(40),
  OuterJoin,
  CrossJoin,
  SingleJoin,
  MarkJoin,

  // isUnaryOperator >= 50
  Binary(50),
  Unary,
  Delete,
  Insert,
  Multiple,
  Project,
  Select,
  Sort,
  Limit,
  Downsample,
  RowTransform,
  SetTransform,
  MappingTransform,
  Rename,
  Reorder,
  AddSchemaPrefix,
  GroupBy,
  Distinct,
  ProjectWaitingForPath,
  ValueToSelectedPath,
  Load;

  private int value;

  OperatorType() {
    this(OperatorTypeCounter.nextValue);
  }

  OperatorType(int value) {
    this.value = value;
    OperatorTypeCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OperatorTypeCounter {

    private static int nextValue = 0;
  }

  public static boolean isBinaryOperator(OperatorType op) {
    return op.value >= 30 && op.value <= 49;
  }

  public static boolean isUnaryOperator(OperatorType op) {
    return op.value >= 50;
  }

  public static boolean isJoinOperator(OperatorType op) {
    return op.value >= 40 && op.value <= 49;
  }

  public static boolean isMultipleOperator(OperatorType op) {
    return op.value >= 10 && op.value <= 19;
  }

  public static boolean isGlobalOperator(OperatorType op) {
    return op == ShowColumns || op == Migration;
  }

  public static boolean isNeedBroadcasting(OperatorType op) {
    return op == Delete || op == Insert;
  }

  public static boolean isSetOperator(OperatorType op) {
    return op.value >= 35 && op.value < 39;
  }
}
