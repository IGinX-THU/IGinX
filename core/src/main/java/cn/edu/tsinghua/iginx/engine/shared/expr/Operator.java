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
package cn.edu.tsinghua.iginx.engine.shared.expr;

public enum Operator {
  PLUS,
  MINUS,
  STAR,
  DIV,
  MOD;

  public static String operatorToString(Operator operator) {
    switch (operator) {
      case PLUS:
        return "+";
      case MINUS:
        return "-";
      case STAR:
        return "×";
      case DIV:
        return "÷";
      case MOD:
        return "%";
      default:
        return "";
    }
  }

  /** 判断两个op是否有相同的优先级 */
  public static boolean hasSamePriority(Operator op1, Operator op2) {
    return ((op1 == PLUS || op1 == MINUS) && (op2 == PLUS || op2 == MINUS))
        || ((op1 == STAR || op1 == DIV) && (op2 == STAR || op2 == DIV))
        || ((op1 == MOD) && (op2 == MOD));
  }

  /** 获取op的相反op */
  public static Operator getOppositeOp(Operator op) {
    switch (op) {
      case PLUS:
        return MINUS;
      case MINUS:
        return PLUS;
      case STAR:
        return DIV;
      case DIV:
        return STAR;
      case MOD:
        return MOD;
    }
    return null;
  }
}
