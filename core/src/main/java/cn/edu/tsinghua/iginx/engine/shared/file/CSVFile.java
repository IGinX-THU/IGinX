package cn.edu.tsinghua.iginx.engine.shared.file;

import cn.edu.tsinghua.iginx.utils.CSVUtils;
import org.apache.commons.csv.CSVFormat;

public class CSVFile {

  private final String filepath;

  private String delimiter;

  private boolean isOptionallyQuote;

  private char quote;

  private char escaped;

  private String recordSeparator;

  public CSVFile(String filepath) {
    this.filepath = filepath;
    this.delimiter = ",";
    this.isOptionallyQuote = false;
    this.quote = '\0';
    this.escaped = '\\';
    this.recordSeparator = "\n";
  }

  public String getFilepath() {
    return filepath;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    if (delimiter.equals("\\t")) {
      this.delimiter = "\t";
    } else {
      this.delimiter = delimiter;
    }
  }

  public boolean isOptionallyQuote() {
    return isOptionallyQuote;
  }

  public void setOptionallyQuote(boolean optionallyQuote) {
    isOptionallyQuote = optionallyQuote;
  }

  public char getQuote() {
    return quote;
  }

  public void setQuote(char quote) {
    this.quote = quote;
  }

  public char getEscaped() {
    return escaped;
  }

  public void setEscaped(char escaped) {
    this.escaped = escaped;
  }

  public String getRecordSeparator() {
    return recordSeparator;
  }

  public void setRecordSeparator(String recordSeparator) {
    switch (recordSeparator) {
      case "\\n":
        this.recordSeparator = "\n";
        break;
      case "\\r":
        this.recordSeparator = "\r";
        break;
      case "\\r\\n":
        this.recordSeparator = "\r\n";
        break;
      default:
        this.recordSeparator = recordSeparator;
        break;
    }
  }

  public CSVFormat.Builder getCSVBuilder() {
    return CSVUtils.getCSVBuilder(delimiter, isOptionallyQuote, quote, escaped, recordSeparator);
  }
}
