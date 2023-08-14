package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;
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
        break;
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
        return new KeyInterval(keys.get(0), keys.get(keys.size() - 1));
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
