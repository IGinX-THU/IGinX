package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;

public class InsertFromCsvStatement extends DataStatement {

  private final ImportFile importFile;

  private final InsertStatement subInsertStatement;

  private final long keyBase;

  private final String keyCol;

  public InsertFromCsvStatement(
      ImportFile importFile, InsertStatement subInsertStatement, long keyBase, String keyCol) {
    this.statementType = StatementType.INSERT_FROM_CSV;
    this.importFile = importFile;
    this.subInsertStatement = subInsertStatement;
    this.keyBase = keyBase;
    this.keyCol = keyCol;
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

  public String getKeyCol() {
    return keyCol;
  }
}
