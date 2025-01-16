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
package cn.edu.tsinghua.iginx.engine.shared.expr;

public enum Operator {
  PLUS,
  MINUS,
  STAR, // 乘法展示, ×
  DIV, // 除法展示, ÷
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

  public static String operatorToCalString(Operator operator) {
    switch (operator) {
      case STAR:
        return "*";
      case DIV:
        return "/";
      default:
        return operatorToString(operator);
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
