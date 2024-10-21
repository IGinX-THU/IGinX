/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
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
