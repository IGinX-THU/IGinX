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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.*;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.*;
import cn.edu.tsinghua.iginx.sql.statement.select.*;
import java.util.*;

/** Checks explicit references before side effects, then narrows storage plans before execution. */
public final class PathAuthorizer {
  private final SecurityConfig config;
  private final RequestContext context;
  private List<String> visibleColumns;

  public PathAuthorizer(RequestContext context) {
    this.config = SecurityConfig.getInstance();
    this.context = context;
  }

  public boolean restricted() {
    return false; // Reserved path filtering is introduced by PR 3.
  }

  public void checkPaths(Collection<String> paths) {
    // Recursive statement permissions are enforced by check().
  }

  public void check(Statement statement) {
    if (!config.isEnabled()) return;
    check(statement, Collections.newSetFromMap(new IdentityHashMap<Statement, Boolean>()));
  }

  private void check(Statement statement, Set<Statement> visited) {
    // CTE bodies share the parser's CTE list, which can refer back to the same statement.
    // Keep the traversal request-local while still checking every distinct nested statement.
    if (!visited.add(statement)) return;
    if (!context.isInternal()
        && !StatementPermissions.permits(context.getSessionId(), statement.getType())) {
      throw new SecurityException("Statement permission denied");
    }
    if (statement instanceof InsertFromSelectStatement) {
      check(((InsertFromSelectStatement) statement).getSubSelectStatement(), visited);
      check(((InsertFromSelectStatement) statement).getSubInsertStatement(), visited);
    } else if (statement instanceof InsertFromCsvStatement) {
      check(((InsertFromCsvStatement) statement).getSubInsertStatement(), visited);
    } else if (statement instanceof ExportFileFromSelectStatement) {
      check(((ExportFileFromSelectStatement) statement).getSelectStatement(), visited);
    } else if (statement instanceof InsertStatement) {
      InsertStatement insert = (InsertStatement) statement;
      if (insert.getPrefixPath() != null)
        checkPaths(Collections.singletonList(insert.getPrefixPath()));
      checkPaths(insert.getPaths());
    } else if (statement instanceof DeleteStatement) {
      checkPaths(((DeleteStatement) statement).getPaths());
    } else if (statement instanceof DeleteColumnsStatement) {
      checkPaths(((DeleteColumnsStatement) statement).getPaths());
    } else if (statement instanceof ShowColumnsStatement) {
      checkPaths(((ShowColumnsStatement) statement).getPathRegexSet());
    } else if (statement instanceof SelectStatement) {
      SelectStatement select = (SelectStatement) statement;
      checkPaths(select.getPathSet());
      for (CommonTableExpression cte : select.getCteList()) check(cte.getStatement(), visited);
      if (select instanceof BinarySelectStatement) {
        check(((BinarySelectStatement) select).getLeftQuery(), visited);
        check(((BinarySelectStatement) select).getRightQuery(), visited);
      } else if (select instanceof UnarySelectStatement) {
        UnarySelectStatement unary = (UnarySelectStatement) select;
        List<FromPart> parts = new ArrayList<>(unary.getFromParts());
        parts.addAll(unary.getSelectSubQueryParts());
        parts.addAll(unary.getWhereSubQueryParts());
        parts.addAll(unary.getHavingSubQueryParts());
        for (FromPart part : parts) {
          if (part instanceof SubQueryFromPart)
            check(((SubQueryFromPart) part).getSubQuery(), visited);
          if (part instanceof ShowColumnsFromPart)
            check(((ShowColumnsFromPart) part).getShowColumnsStatement(), visited);
        }
      }
    }
  }

  public List<String> resolve(Collection<String> paths) throws PhysicalException {
    return new ArrayList<>(paths);
  }

  public Operator rewrite(Operator operator) throws PhysicalException {
    return operator;
  }
}
