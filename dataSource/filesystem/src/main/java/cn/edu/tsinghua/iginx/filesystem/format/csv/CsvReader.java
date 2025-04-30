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
package cn.edu.tsinghua.iginx.filesystem.format.csv;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.*;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvReader implements FileFormat.Reader {

  private final static Logger LOGGER = LoggerFactory.getLogger(CsvReader.class);

  private final Path path;
  private final CsvReaderConfig config;
  private final CsvDataType[] csvDataType;
  private final String[] headers;
  private final Map<String, Integer> fieldIndex;
  private final CSVFormat csvFormat;
  private final DateTimeFormatter dateFormat;

  public CsvReader(String prefix, Path path, CsvReaderConfig config) throws IOException {
    this.path = path;
    this.config = config;

    // Determine CSV format based on file extension
    String delimiter;
    if(config.getDelimiter() != null) {
      delimiter = config.getDelimiter();
    }else{
      boolean isTsv = path.toFile().getName().endsWith(".tsv");
      delimiter = isTsv ? "\t" : ",";
    }
    this.csvFormat =
        CSVFormat.DEFAULT
            .builder()
            .setDelimiter(delimiter)
            .setHeader() // Tell parser to treat first row as header
            .build();
    this.dateFormat = DateTimeFormatter.ofPattern(config.getDateFormat());

    // Read headers and infer schema
    List<CSVRecord> sample;
    try (BufferedReader reader = Files.newBufferedReader(path)) {
      CSVParser parser = CSVParser.parse(reader, csvFormat);
      sample = parser.stream().limit(config.getSampleSize() + 1).collect(Collectors.toList());
      this.headers = new String[parser.getHeaderNames().size()];
      this.csvDataType = new CsvDataType[headers.length];
      parserHeader(parser.getHeaderNames(),prefix);
    }

    if(config.isInferSchema()){
      inferSchema(sample);
    }

    this.fieldIndex = new HashMap<>();
    for (int i = 0; i < headers.length; i++) {
      fieldIndex.put(headers[i], i);
    }
  }

  private void parserHeader(List<String> headerNames, String prefix) {
    for (int i = 0; i < headerNames.size(); i++) {
      String headerName = headerNames.get(i);
      String name = headerName;
      CsvDataType type = CsvDataType.UNKNOWN;
      if(config.isParseTypeFromHeader()){
        final int colon = headerName.indexOf(':');
        if (colon >= 0) {
          name = headerName.substring(0, colon);
          String typeString = headerName.substring(colon + 1);
          type = parseType(typeString.toLowerCase());
        }
      }
      if (prefix != null && !prefix.isEmpty()) {
        this.headers[i] = prefix + "." + name;
      } else {
        this.headers[i] = name;
      }
      this.csvDataType[i] = type;
    }
  }

  private static final Pattern DECIMAL_TYPE_PATTERN = Pattern
      .compile("\"decimal\\(([0-9]+),([0-9]+)\\)");
  private CsvDataType parseType(String typeString) {
    Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(typeString);
    if (decimalMatcher.matches()) {
      return CsvDataType.DOUBLE;
    } else {
      switch (typeString) {
        case "boolean":
          return CsvDataType.BOOLEAN;
        case "int":
          return CsvDataType.INT;
        case "long":
          return CsvDataType.LONG;
        case "float":
          return CsvDataType.FLOAT;
        case "double":
          return CsvDataType.DOUBLE;
        case "string":
          return CsvDataType.STRING;
        case "date":
          return CsvDataType.DATE;
        default:
          LOGGER.warn("Found unknown type hint: {} in file: {}", typeString, path);
          return CsvDataType.UNKNOWN;
      }
    }
  }

  private void inferSchema(List<CSVRecord> sample) {
    boolean[] couldBeBoolean = new boolean[headers.length];
    boolean[] couldBeLong = new boolean[headers.length];
    boolean[] couldBeDouble = new boolean[headers.length];
    boolean[] couldBeDate = new boolean[headers.length];

    Arrays.fill(couldBeBoolean, true);
    Arrays.fill(couldBeLong, true);
    Arrays.fill(couldBeDouble, true);
    Arrays.fill(couldBeDate, true);

    for (CSVRecord record : sample) {
      for (int i = 0; i < Math.min(headers.length, record.size()); i++) {
        if(this.csvDataType[i] != CsvDataType.UNKNOWN) {
          continue;
        }
        String value = record.get(i);

        // Skip empty values during type inference
        if (value == null || value.trim().isEmpty()) {
          continue;
        }

        if (couldBeBoolean[i]) {
          if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            couldBeBoolean[i] = false;
          }
        }

        if (couldBeLong[i]) {
          try {
            Long.parseLong(value);
          } catch (NumberFormatException e) {
            couldBeLong[i] = false;
          }
        }

        if (couldBeDouble[i]) {
          try {
            Double.parseDouble(value);
          } catch (NumberFormatException e) {
            couldBeDouble[i] = false;
          }
        }

        if (couldBeDate[i]) {
          try {
            // Use the same parsing logic that will be used in parseValue
            if (config.getDateFormat().contains("HH") || config.getDateFormat().contains("mm")) {
              LocalDateTime.parse(value, dateFormat);
            } else {
              LocalDate.parse(value, dateFormat);
            }
          } catch (DateTimeParseException e) {
            couldBeDate[i] = false;
          }
        }
      }
    }

    // Determine types based on inference results
    for (int i = 0; i < headers.length; i++) {
      if(this.csvDataType[i] != CsvDataType.UNKNOWN) {
        continue;
      }
      if (couldBeBoolean[i]) {
        csvDataType[i] = CsvDataType.BOOLEAN;
      } else if (couldBeLong[i]) {
        csvDataType[i] = CsvDataType.LONG;
      } else if (couldBeDouble[i]) {
        csvDataType[i] = CsvDataType.DOUBLE;
      } else if (couldBeDate[i]) {
        csvDataType[i] = CsvDataType.DATE;
      } else {
        csvDataType[i] = CsvDataType.STRING;
      }
    }
  }

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    Map<String, DataType> result = new HashMap<>();
    for (String field : fieldIndex.keySet()) {
      if (Patterns.match(patterns, field)) {
        result.put(field, csvDataType[fieldIndex.get(field)].getDataType());
      }
    }
    return result;
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    int[] project =
        fields.stream()
            .map(fieldIndex::get)
            .filter(Objects::nonNull)
            .mapToInt(Integer::intValue)
            .toArray();

    Predicate<Filter> removeNonKeyFilter = Filters.nonKeyFilter();

    Filter keyRangeFilter = Filters.superSet(filter, removeNonKeyFilter);
    RangeSet<Long> keyRanges = Filters.toRangeSet(keyRangeFilter);
    RowStream rowStream = new CsvFormatRowStream(project, keyRanges);

    if (!Filters.match(filter, removeNonKeyFilter)) {
      rowStream = RowStreams.filtered(rowStream, filter);
    }

    return rowStream;
  }

  @Override
  public void close() throws IOException {
    // No resources to close
  }

  private class CsvFormatRowStream extends FileSystemRowStream {

    private final int[] project;
    private final BufferedReader reader;
    private final Iterator<CSVRecord> recordIterator;
    private final Header header;
    private final Queue<Range<Long>> keyRanges;

    private long nextIndex = 0;
    private Row nextRow = null;

    public CsvFormatRowStream(int[] project, RangeSet<Long> keyRanges) throws IOException {
      this.project = project;

      this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
      CSVParser parser = new CSVParser(reader, csvFormat);
      this.recordIterator = parser.iterator();

      List<Field> fields = new ArrayList<>();
      for (int index : project) {
        String fieldName = headers[index];
        DataType dataType = csvDataType[index].getDataType();
        fields.add(new Field(fieldName, dataType));
      }
      this.header = new Header(Field.KEY, fields);

      // Initialize key ranges
      this.keyRanges = new ArrayDeque<>();
      for (Range<Long> range : keyRanges.asRanges()) {
        Range<Long> closedRange = Ranges.toClosedLongRange(range);
        if (!closedRange.isEmpty()) {
          this.keyRanges.add(closedRange);
        }
      }
    }

    @Override
    public void close() throws FileSystemException {
      try {
        reader.close();
      } catch (IOException e) {
        throw new FileSystemException(e);
      }
    }

    @Override
    public Header getHeader() throws FileSystemException {
      return header;
    }

    @Override
    public boolean hasNext() throws FileSystemException {
      if (nextRow == null) {
        nextRow = fetchNext();
      }
      return nextRow != null;
    }

    @Override
    public Row next() throws FileSystemException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row row = nextRow;
      nextRow = fetchNext();
      return row;
    }

    private Row fetchNext() throws FileSystemException {
      if (keyRanges.isEmpty()) {
        return null;
      }
      while (recordIterator.hasNext()) {
        CSVRecord record = recordIterator.next();
        try {
          if (nextIndex > keyRanges.peek().upperEndpoint()) {
            keyRanges.poll();
            if (keyRanges.isEmpty()) {
              return null;
            }
          }
          if (nextIndex < keyRanges.peek().lowerEndpoint()) {
            continue;
          }
          return parseRow(nextIndex, record);
        } finally {
          nextIndex++;
        }
      }
      keyRanges.clear();
      return null;
    }

    private Row parseRow(long rowIndex, CSVRecord record) throws FileSystemException {
      Object[] rowValues = new Object[project.length];
      for (int i = 0; i < project.length; i++) {
        int index = project[i];
        CsvDataType dataType = csvDataType[index];
        String value = record.get(index);
        rowValues[i] = parseValue(value, dataType);
      }

      return new Row(header, rowIndex, rowValues);
    }

    private Object parseValue(String value, CsvDataType dataType) {
      if (value == null) {
        return null;
      }
      switch (dataType) {
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        case INT:
          try {
            return Integer.parseInt(value);
          } catch (NumberFormatException e) {
            return null;
          }
        case LONG:
          try {
            return Long.parseLong(value);
          } catch (NumberFormatException e) {
            return null;
          }
        case FLOAT:
          try {
            return Float.parseFloat(value);
          } catch (NumberFormatException e) {
            return null;
          }
        case DOUBLE:
          try {
            return Double.parseDouble(value);
          } catch (NumberFormatException e) {
            return null;
          }
        case DATE:
          try {
            return LocalDate.parse(value, dateFormat)
                .atStartOfDay()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
          } catch (DateTimeParseException e) {
            return null;
          }
        case STRING:
        case UNKNOWN:
        default:
          return value.getBytes();
      }
    }
  }
}
