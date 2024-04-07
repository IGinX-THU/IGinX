package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;

public class InsertFromCsvStatement extends DataStatement {

  private final ImportFile importFile;

  private final InsertStatement subInsertStatement;

  private final long keyBase;

  public InsertFromCsvStatement(
      ImportFile importFile, InsertStatement subInsertStatement, long keyBase) {
    this.statementType = StatementType.INSERT_FROM_CSV;
    this.importFile = importFile;
    this.subInsertStatement = subInsertStatement;
    this.keyBase = keyBase;
  }

  public InsertFromCsvStatement(ImportFile importFile, InsertStatement subInsertStatement) {
    this(importFile, subInsertStatement, 0);
  }

  public ImportFile getImportFile() {
    return importFile;
  }

  public InsertStatement getSubInsertStatement() {
    return subInsertStatement;
  }

  public long getKeyBase() {
    return keyBase;
  }
}
