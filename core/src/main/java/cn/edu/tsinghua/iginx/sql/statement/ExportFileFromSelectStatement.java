package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement;

public class ExportFileFromSelectStatement extends DataStatement {

  private final SelectStatement selectStatement;

  private final ExportFile exportFile;

  public ExportFileFromSelectStatement(SelectStatement selectStatement, ExportFile exportFile) {
    this.statementType = StatementType.EXPORT_FILE_FROM_SELECT;
    this.selectStatement = selectStatement;
    this.exportFile = exportFile;
  }

  public SelectStatement getSelectStatement() {
    return selectStatement;
  }

  public ExportFile getExportFile() {
    return exportFile;
  }
}
