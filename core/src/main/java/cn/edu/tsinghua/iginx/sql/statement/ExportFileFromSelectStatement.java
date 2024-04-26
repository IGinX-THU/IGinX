package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.FileType;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class ExportFileFromSelectStatement extends DataStatement {

  private final SelectStatement selectStatement;

  private final ExportFile exportFile;

  public ExportFileFromSelectStatement(SelectStatement selectStatement, ExportFile exportFile) {
    if (exportFile.getType().equals(FileType.CSV)) {
      this.statementType = StatementType.EXPORT_CSV_FROM_SELECT;
    } else if (exportFile.getType().equals(FileType.BYTE_STREAM)) {
      this.statementType = StatementType.EXPORT_STREAM_FROM_SELECT;
    }
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
