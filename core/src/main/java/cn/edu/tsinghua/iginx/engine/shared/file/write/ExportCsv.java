package cn.edu.tsinghua.iginx.engine.shared.file.write;

import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.FileType;

public class ExportCsv implements ExportFile {

  private boolean isExportHeader;

  private final CSVFile csvFile;

  public ExportCsv(String filepath) {
    this.csvFile = new CSVFile(filepath);
    this.isExportHeader = false;
  }

  public boolean isExportHeader() {
    return isExportHeader;
  }

  public void setExportHeader(boolean exportHeader) {
    isExportHeader = exportHeader;
  }

  public CSVFile getCsvFile() {
    return csvFile;
  }

  public String getFilepath() {
    return csvFile.getFilepath();
  }

  @Override
  public FileType getType() {
    return FileType.CSV;
  }
}
