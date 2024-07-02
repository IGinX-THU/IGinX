/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.thrift.ExportCSV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class CSVUtils {

  public static CSVFormat.Builder getCSVBuilder(ExportCSV exportCSV) {
    return getCSVBuilder(
        exportCSV.delimiter,
        exportCSV.isOptionallyQuote,
        (char) exportCSV.quote,
        (char) exportCSV.escaped,
        exportCSV.recordSeparator);
  }

  public static CSVFormat.Builder getCSVBuilder(
      String delimiter,
      boolean isOptionallyQuote,
      char quote,
      char escaped,
      String recordSeparator) {
    CSVFormat.Builder builder = CSVFormat.Builder.create(CSVFormat.DEFAULT);
    builder.setDelimiter(delimiter);
    builder.setQuote(quote);
    if (quote == '\0') {
      builder.setQuoteMode(QuoteMode.NONE);
    } else if (isOptionallyQuote) {
      builder.setQuoteMode(QuoteMode.NON_NUMERIC);
    } else {
      builder.setQuoteMode(QuoteMode.ALL);
    }
    builder.setEscape(escaped);
    builder.setRecordSeparator(recordSeparator);
    return builder;
  }
}
