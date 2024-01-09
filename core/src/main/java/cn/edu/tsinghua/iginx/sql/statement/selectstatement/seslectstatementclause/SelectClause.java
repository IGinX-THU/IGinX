package cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause;

import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.List;

public class SelectClause {
  private final List<SubQueryFromPart> selectSubQueryParts;
  private boolean isDistinct = false;

  private boolean hasValueToSelectedPath = false;

  public SelectClause() {
    this.selectSubQueryParts = new ArrayList<>();
  }

  public void addSelectSubQueryPart(SubQueryFromPart selectSubQueryPart) {
    this.selectSubQueryParts.add(selectSubQueryPart);
  }

  public List<SubQueryFromPart> getSelectSubQueryParts() {
    return selectSubQueryParts;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  public boolean hasValueToSelectedPath() {
    return hasValueToSelectedPath;
  }

  public void setHasValueToSelectedPath(boolean hasValueToSelectedPath) {
    this.hasValueToSelectedPath = hasValueToSelectedPath;
  }
}
