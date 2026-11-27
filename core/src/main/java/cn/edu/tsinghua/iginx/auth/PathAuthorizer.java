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
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.*;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.*;
import cn.edu.tsinghua.iginx.sql.statement.select.*;
import java.nio.charset.StandardCharsets;
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
    return config.isEnabled()
        && !config.getReservedPrefixes().isEmpty()
        && (context.isInternal()
            || !config.allowsReserved(
                SessionManager.getInstance().getUsername(context.getSessionId())));
  }

  public static boolean isReserved(String path, List<String> prefixes) {
    // SQL parsing has already unquoted identifiers. Also normalize wire-level quoted paths.
    String normalized = path.replace("`", "");
    for (String prefix : prefixes) {
      if (normalized.equals(prefix)
          || normalized.startsWith(prefix + ".")
          || normalized.startsWith(prefix + "{")) return true;
    }
    return false;
  }

  public void checkPaths(Collection<String> paths) {
    if (!restricted()) return;
    for (String path : paths) {
      if (isReserved(path, config.getReservedPrefixes()))
        throw new SecurityException("Reserved path access denied");
    }
  }

  public void check(Statement statement) {
    if (!config.isEnabled()) return;
    if (!context.isInternal()
        && !StatementPermissions.permits(context.getSessionId(), statement.getType())) {
      throw new SecurityException("Statement permission denied");
    }
    if (statement instanceof InsertFromSelectStatement) {
      check(((InsertFromSelectStatement) statement).getSubSelectStatement());
      check(((InsertFromSelectStatement) statement).getSubInsertStatement());
    } else if (statement instanceof InsertFromCsvStatement) {
      check(((InsertFromCsvStatement) statement).getSubInsertStatement());
    } else if (statement instanceof ExportFileFromSelectStatement) {
      check(((ExportFileFromSelectStatement) statement).getSelectStatement());
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
      for (CommonTableExpression cte : select.getCteList()) check(cte.getStatement());
      if (select instanceof BinarySelectStatement) {
        check(((BinarySelectStatement) select).getLeftQuery());
        check(((BinarySelectStatement) select).getRightQuery());
      } else if (select instanceof UnarySelectStatement) {
        UnarySelectStatement unary = (UnarySelectStatement) select;
        List<FromPart> parts = new ArrayList<>(unary.getFromParts());
        parts.addAll(unary.getSelectSubQueryParts());
        parts.addAll(unary.getWhereSubQueryParts());
        parts.addAll(unary.getHavingSubQueryParts());
        for (FromPart part : parts) {
          if (part instanceof SubQueryFromPart) check(((SubQueryFromPart) part).getSubQuery());
          if (part instanceof ShowColumnsFromPart)
            check(((ShowColumnsFromPart) part).getShowColumnsStatement());
        }
      }
    }
  }

  static boolean needsExpansion(String pattern, List<String> prefixes) {
    pattern = pattern.replace("`", "");
    int wildcard = pattern.indexOf('*');
    if (wildcard < 0) return false;
    String fixed = pattern.substring(0, wildcard);
    for (String prefix : prefixes) {
      if (prefix.startsWith(fixed)
          || (prefix + ".").startsWith(fixed)
          || fixed.startsWith(prefix + ".")) return true;
    }
    return false;
  }

  public List<String> resolve(Collection<String> paths) throws PhysicalException {
    if (!restricted()) return new ArrayList<>(paths);
    checkPaths(paths);
    Set<String> result = new LinkedHashSet<>();
    for (String pattern : paths) {
      if (!needsExpansion(pattern, config.getReservedPrefixes())) {
        result.add(pattern);
      } else {
        if (visibleColumns == null) {
          visibleColumns = new ArrayList<>();
          ShowColumns all =
              new ShowColumns(
                  new GlobalSource(), Collections.singleton("*"), null, Integer.MAX_VALUE, 0);
          RowStream stream =
              StoragePhysicalTaskExecutor.getInstance().executeShowColumns(all).getRowStream();
          try {
            while (stream.hasNext()) {
              String path =
                  cn.edu.tsinghua.iginx.utils.TagKVUtils.fromFullName(
                          new String((byte[]) stream.next().getValues()[0], StandardCharsets.UTF_8))
                      .getK();
              if (!isReserved(path, config.getReservedPrefixes())) visibleColumns.add(path);
            }
          } finally {
            stream.close();
          }
        }
        for (String path : visibleColumns)
          if (SecurityConfig.globMatches(pattern, path)) result.add(path);
      }
    }
    return new ArrayList<>(result);
  }

  public Operator rewrite(Operator operator) throws PhysicalException {
    if (operator == null || !restricted()) return operator;
    if (operator instanceof ShowColumns) {
      ShowColumns show = (ShowColumns) operator;
      Collection<String> patterns =
          show.getPathRegexSet().isEmpty() ? Collections.singleton("*") : show.getPathRegexSet();
      Set<String> visible = new HashSet<>(resolve(patterns));
      return new ShowColumns(
          new GlobalSource(),
          visible,
          show.getTagFilter(),
          show.getLimit(),
          show.getOffset(),
          visible.isEmpty());
    }
    if (operator instanceof Project) {
      Project project = (Project) operator;
      if (project.getSource() instanceof FragmentSource
          || project.getSource() instanceof GlobalSource) {
        project.setPatterns(resolve(project.getPatterns()));
      }
    }
    if (operator instanceof Delete) {
      Delete delete = (Delete) operator;
      List<String> paths = resolve(delete.getPatterns());
      return new Delete(
          (FragmentSource) delete.getSource(), delete.getKeyRanges(), paths, delete.getTagFilter());
    }
    if (operator instanceof Insert) checkPaths(((Insert) operator).getData().getPaths());
    if (operator instanceof UnaryOperator) {
      UnaryOperator unary = (UnaryOperator) operator;
      unary.setSource(rewriteSource(unary.getSource()));
    } else if (operator instanceof BinaryOperator) {
      BinaryOperator binary = (BinaryOperator) operator;
      binary.setSourceA(rewriteSource(binary.getSourceA()));
      binary.setSourceB(rewriteSource(binary.getSourceB()));
    } else if (operator instanceof MultipleOperator) {
      MultipleOperator multiple = (MultipleOperator) operator;
      List<Source> sources = new ArrayList<>();
      for (Source source : multiple.getSources()) sources.add(rewriteSource(source));
      multiple.setSources(sources);
    }
    return operator;
  }

  private Source rewriteSource(Source source) throws PhysicalException {
    return source instanceof OperatorSource
        ? new OperatorSource(rewrite(((OperatorSource) source).getOperator()))
        : source;
  }
}
