package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import java.util.ArrayList;
import java.util.List;

public class FromClause {
  private List<FromPart> fromParts;
  private boolean hasJoinParts = false;

  public FromClause() {
    fromParts = new ArrayList<>();
  }

  public void setFromParts(List<FromPart> fromParts) {
    this.fromParts = fromParts;
  }

  public List<FromPart> getFromParts() {
    return fromParts;
  }

  public void setHasJoinParts(boolean hasJoinParts) {
    this.hasJoinParts = hasJoinParts;
  }

  public boolean hasJoinParts() {
    return hasJoinParts;
  }

  public void addFromPart(FromPart fromPart) {
    fromParts.add(fromPart);
  }
}
