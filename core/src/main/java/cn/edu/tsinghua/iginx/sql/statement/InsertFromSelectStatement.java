package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class InsertFromSelectStatement extends DataStatement {

  private final long keyOffset;

  private final SelectStatement subSelectStatement;

  private final InsertStatement subInsertStatement;

  public InsertFromSelectStatement(
      long keyOffset, SelectStatement subSelectStatement, InsertStatement subInsertStatement) {
    this.statementType = StatementType.INSERT_FROM_SELECT;
    this.keyOffset = keyOffset;
    this.subSelectStatement = subSelectStatement;
    this.subInsertStatement = subInsertStatement;
  }

  public long getKeyOffset() {
    return keyOffset;
  }

  public SelectStatement getSubSelectStatement() {
    return subSelectStatement;
  }

  public InsertStatement getSubInsertStatement() {
    return subInsertStatement;
  }
}
