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
package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import org.junit.Test;

public class FilterVisitorTest {

  @Test
  public void testVisit() {
    FilterVisitor visitor = new NaiveVisitor();

    String select = "SELECT a FROM root.v.c WHERE a > 5 AND b <= 10 OR c > 7 AND d == 8;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    Filter filter = statement.getFilter();
    filter.accept(visitor);

    System.out.println();

    select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 or time > 7 AND d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    filter.accept(visitor);
  }

  static class NaiveVisitor implements FilterVisitor {

    @Override
    public void visit(AndFilter filter) {
      System.out.printf("this is and filter with %s children\n", filter.getChildren().size());
    }

    @Override
    public void visit(OrFilter filter) {
      System.out.printf("this is or filter with %s children\n", filter.getChildren().size());
    }

    @Override
    public void visit(NotFilter filter) {
      System.out.printf("this is not filter with a %s child\n", filter.getChild().getType());
    }

    @Override
    public void visit(KeyFilter filter) {
      System.out.printf("this is time filter: [%s]\n", filter.toString());
    }

    @Override
    public void visit(ValueFilter filter) {
      System.out.printf("this is value filter: [%s]\n", filter.toString());
    }

    @Override
    public void visit(PathFilter filter) {
      System.out.printf("this is path filter: [%s]\n", filter.toString());
    }

    @Override
    public void visit(BoolFilter filter) {
      System.out.printf("this is bool filter: [%s]\n", filter.isTrue());
    }

    @Override
    public void visit(ExprFilter filter) {
      System.out.printf("this is expr filter: [%s]\n", filter.toString());
    }

    @Override
    public void visit(InFilter filter) {
      System.out.printf("this is in filter: [%s]\n", filter.toString());
    }
  }
}
