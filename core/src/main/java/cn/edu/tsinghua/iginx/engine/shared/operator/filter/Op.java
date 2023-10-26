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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;

public enum Op {
  // or op (default): [0, 9]
  GE(0),
  G,
  LE,
  L,
  E,
  NE,
  LIKE,
  // and op: [10, 19]
  GE_AND(10),
  G_AND,
  LE_AND,
  L_AND,
  E_AND,
  NE_AND,
  LIKE_AND;

  private int value;

  Op() {
    this(OpCounter.nextValue);
  }

  Op(int value) {
    this.value = value;
    OpCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OpCounter {

    private static int nextValue = 0;
  }

  public static Op getOpposite(Op op) {
    switch (op) {
      case NE:
      case NE_AND:
        return E;
      case E:
      case E_AND:
        return NE;
      case GE:
      case GE_AND:
        return L;
      case LE:
      case LE_AND:
        return G;
      case G:
      case G_AND:
        return LE;
      case L:
      case L_AND:
        return GE;
      case LIKE:
      case LIKE_AND:
        return LIKE;
      default:
        return op;
    }
  }

  public static Op getDeMorganOpposite(Op op) {
    switch (op) {
      case NE:
        return E_AND;
      case E:
        return NE_AND;
      case GE:
        return L_AND;
      case LE:
        return G_AND;
      case G:
        return LE_AND;
      case L:
        return GE_AND;
      case LIKE:
        return LIKE_AND;
      case NE_AND:
        return E;
      case E_AND:
        return NE;
      case GE_AND:
        return L;
      case LE_AND:
        return G;
      case G_AND:
        return LE;
      case L_AND:
        return GE;
      case LIKE_AND:
        return LIKE;
      default:
        return op;
    }
  }

  public static Op getDirectionOpposite(Op op) {
    switch (op) {
      case GE:
        return LE;
      case LE:
        return GE;
      case G:
        return L;
      case L:
        return G;
      case GE_AND:
        return LE_AND;
      case LE_AND:
        return GE_AND;
      case G_AND:
        return L_AND;
      case L_AND:
        return G_AND;
      default:
        return op;
    }
  }

  public static Op str2Op(String op) {
    switch (op) {
      case "=":
      case "==":
      case "|=":
      case "|==":
        return E;
      case "!=":
      case "<>":
      case "|!=":
      case "|<>":
        return NE;
      case ">":
      case "|>":
        return G;
      case ">=":
      case "|>=":
        return GE;
      case "<":
      case "|<":
        return L;
      case "<=":
      case "|<=":
        return LE;
      case "like":
      case "|like":
        return LIKE;
      case "&=":
      case "&==":
        return E_AND;
      case "&!=":
      case "&<>":
        return NE_AND;
      case "&>":
        return G_AND;
      case "&>=":
        return GE_AND;
      case "&<":
        return L_AND;
      case "&<=":
        return LE_AND;
      case "&like":
        return LIKE_AND;
      default:
        throw new SQLParserException(String.format("Not support comparison operator %s", op));
    }
  }

  public static String op2Str(Op op) {
    switch (op) {
      case GE:
        return ">=";
      case G:
        return ">";
      case LE:
        return "<=";
      case L:
        return "<";
      case E:
        return "==";
      case NE:
        return "!=";
      case LIKE:
        return "like";
      case GE_AND:
        return "&>=";
      case G_AND:
        return "&>";
      case LE_AND:
        return "&<=";
      case L_AND:
        return "&<";
      case E_AND:
        return "&==";
      case NE_AND:
        return "&!=";
      case LIKE_AND:
        return "&like";
      default:
        return "";
    }
  }

  public static boolean isEqualOp(Op op) {
    return op.equals(E) || op.equals(E_AND);
  }

  public static boolean isLikeOp(Op op) {
    return op.equals(LIKE) || op.equals(LIKE_AND);
  }

  public static boolean isOrOp(Op op) {
    return op.value >= 0 && op.value <= 9;
  }

  public static boolean isAndOp(Op op) {
    return op.value >= 10 && op.value <= 19;
  }
}
