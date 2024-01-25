package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

public class LimitClause {
  private int limit;
  private int offset;

  public LimitClause(int limit, int offset) {
    this.limit = limit;
    this.offset = offset;
  }

  public LimitClause() {
    this.limit = Integer.MAX_VALUE;
    this.offset = 0;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
