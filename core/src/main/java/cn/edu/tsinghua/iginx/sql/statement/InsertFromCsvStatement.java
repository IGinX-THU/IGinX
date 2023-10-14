package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;

public class InsertFromCsvStatement extends DataStatement {

  private final ImportFile importFile;

  private final InsertStatement subInsertStatement;

  public InsertFromCsvStatement(ImportFile importFile, InsertStatement subInsertStatement) {
    this.statementType = StatementType.INSERT_FROM_CSV;
    this.importFile = importFile;
    this.subInsertStatement = subInsertStatement;
  }

  public ImportFile getImportFile() {
    return importFile;
  }

  public InsertStatement getSubInsertStatement() {
    return subInsertStatement;
  }
}
