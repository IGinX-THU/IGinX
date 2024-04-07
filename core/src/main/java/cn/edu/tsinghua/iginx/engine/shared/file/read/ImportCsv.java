package cn.edu.tsinghua.iginx.engine.shared.file.read;

import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.FileType;
import org.apache.commons.csv.CSVFormat;

public class ImportCsv implements ImportFile {

  public static String DEFAULT_CHARSET = "UTF-8";

  private boolean isSkippingImportHeader;

  private final CSVFile csvFile;

  public ImportCsv(String filepath) {
    this.csvFile = new CSVFile(filepath);
    this.isSkippingImportHeader = false;
  }

  public boolean isSkippingImportHeader() {
    return isSkippingImportHeader;
  }

  public void setSkippingImportHeader(boolean skippingImportHeader) {
    isSkippingImportHeader = skippingImportHeader;
  }

  public CSVFile getCsvFile() {
    return csvFile;
  }

  public String getFilepath() {
    return csvFile.getFilepath();
  }

  public CSVFormat.Builder getCSVBuilder() {
    return csvFile.getCSVBuilder();
  }

  @Override
  public FileType getType() {
    return FileType.CSV;
  }
}
