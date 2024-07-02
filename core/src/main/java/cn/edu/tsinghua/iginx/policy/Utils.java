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
package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import java.util.*;

public class Utils {

  public static List<String> getPathListFromStatement(DataStatement statement) {
    switch (statement.getType()) {
      case SELECT:
        return new ArrayList<>(((SelectStatement) statement).getPathSet());
      case DELETE:
        return ((DeleteStatement) statement).getPaths();
      case INSERT:
        return ((InsertStatement) statement).getPaths();
      default:
        // TODO: case label. should we return empty list for other statements?
    }
    return Collections.emptyList();
  }

  public static List<String> getNonWildCardPaths(List<String> paths) {
    Set<String> beCutPaths = new TreeSet<>();
    for (String path : paths) {
      if (!path.contains(Constants.LEVEL_PLACEHOLDER)) {
        beCutPaths.add(path);
        continue;
      }
      String[] parts = path.split("\\" + Constants.LEVEL_SEPARATOR);
      if (parts.length == 0 || parts[0].equals(Constants.LEVEL_PLACEHOLDER)) {
        continue;
      }
      StringBuilder pathBuilder = new StringBuilder();
      for (String part : parts) {
        if (part.equals(Constants.LEVEL_PLACEHOLDER)) {
          break;
        }
        if (pathBuilder.length() != 0) {
          pathBuilder.append(Constants.LEVEL_SEPARATOR);
        }
        pathBuilder.append(part);
      }
      beCutPaths.add(pathBuilder.toString());
    }
    return new ArrayList<>(beCutPaths);
  }

  public static KeyInterval getKeyIntervalFromDataStatement(DataStatement statement) {
    StatementType type = statement.getType();
    switch (type) {
      case INSERT:
        InsertStatement insertStatement = (InsertStatement) statement;
        List<Long> keys = insertStatement.getKeys();
        return new KeyInterval(
            Collections.min(keys), Collections.min(keys)); // interval should require coparison
      case SELECT:
        UnarySelectStatement selectStatement = (UnarySelectStatement) statement;
        return new KeyInterval(selectStatement.getStartKey(), selectStatement.getEndKey());
      case DELETE:
        DeleteStatement deleteStatement = (DeleteStatement) statement;
        List<KeyRange> keyRanges = deleteStatement.getKeyRanges();
        long startKey = Long.MAX_VALUE, endKey = Long.MIN_VALUE;
        for (KeyRange keyRange : keyRanges) {
          if (keyRange.getBeginKey() < startKey) {
            startKey = keyRange.getBeginKey();
          }
          if (keyRange.getEndKey() > endKey) {
            endKey = keyRange.getEndKey();
          }
        }
        startKey = startKey == Long.MAX_VALUE ? 0 : startKey;
        endKey = endKey == Long.MIN_VALUE ? Long.MAX_VALUE : endKey;
        return new KeyInterval(startKey, endKey);
      default:
        return new KeyInterval(0, Long.MAX_VALUE);
    }
  }
}
