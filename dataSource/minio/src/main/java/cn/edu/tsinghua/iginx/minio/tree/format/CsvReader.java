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
package cn.edu.tsinghua.iginx.minio.tree.format;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.minio.common.Patterns;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CsvReader implements ObjectFormat.Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvReader.class);

  private static final Pattern DECIMAL_TYPE = Pattern.compile("\"decimal\\(([0-9]+),([0-9]+)\\)");

  /** type of a csv column, dates are stored as LONG */
  private enum ColumnType {
    UNKNOWN(DataType.BINARY),
    INT(DataType.INTEGER),
    LONG(DataType.LONG),
    FLOAT(DataType.FLOAT),
    DOUBLE(DataType.DOUBLE),
    BOOLEAN(DataType.BOOLEAN),
    DATE(DataType.LONG),
    STRING(DataType.BINARY);

    private final DataType dataType;

    ColumnType(DataType dataType) {
      this.dataType = dataType;
    }
  }

  static final class Options {
    boolean parseTypeFromHeader = true;
    boolean inferSchema = false;
    @Nullable String delimiter = null;
    int sampleSize = 1;
    String dateFormat = "yyyy-MM-dd";
    boolean allowDuplicateColumnNames = false;

    static Options of(Config config) {
      Options options = new Options();
      options.parseTypeFromHeader =
          Configs.getBoolean(config, "parseTypeFromHeader", options.parseTypeFromHeader);
      options.inferSchema = Configs.getBoolean(config, "inferSchema", options.inferSchema);
      options.delimiter = Configs.getString(config, "delimiter", options.delimiter);
      options.sampleSize = Configs.getInt(config, "sampleSize", options.sampleSize);
      options.dateFormat = Configs.getString(config, "dateFormat", options.dateFormat);
      options.allowDuplicateColumnNames =
          Configs.getBoolean(
              config, "allowDuplicateColumnNames", options.allowDuplicateColumnNames);
      if (options.sampleSize <= 0) {
        throw new IllegalArgumentException("sampleSize should be greater than 0");
      }
      return options;
    }
  }

  private final Path path;
  private final Options options;
  private final CSVFormat csvFormat;
  private final DateTimeFormatter dateFormatter;
  private final String[] names;
  private final ColumnType[] types;
  private final Map<String, Integer> indexes = new HashMap<>();

  CsvReader(@Nullable String prefix, Path path, Options options) throws IOException {
    this.path = path;
    this.options = options;
    String delimiter = options.delimiter;
    if (delimiter == null) {
      delimiter = path.getFileName().toString().endsWith(".tsv") ? "\t" : ",";
    }
    this.csvFormat = CSVFormat.DEFAULT.builder().setDelimiter(delimiter).setHeader().build();
    this.dateFormatter = DateTimeFormatter.ofPattern(options.dateFormat);

    List<CSVRecord> sample;
    List<String> headerNames;
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        CSVParser parser = CSVParser.parse(reader, csvFormat)) {
      sample = parser.stream().limit(options.sampleSize + 1).collect(Collectors.toList());
      headerNames = parser.getHeaderNames();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
    this.names = new String[headerNames.size()];
    this.types = new ColumnType[headerNames.size()];
    parseHeader(headerNames, prefix);
    if (options.inferSchema) {
      inferSchema(sample);
    }
    indexColumns();
  }

  private void parseHeader(List<String> headerNames, @Nullable String prefix) {
    for (int i = 0; i < headerNames.size(); i++) {
      String name = headerNames.get(i);
      ColumnType type = ColumnType.UNKNOWN;
      if (options.parseTypeFromHeader) {
        int colon = name.indexOf(':');
        if (colon >= 0) {
          type = parseType(name.substring(colon + 1).toLowerCase());
          name = name.substring(0, colon);
        }
      }
      names[i] = prefix == null || prefix.isEmpty() ? name : prefix + "." + name;
      types[i] = type;
    }
  }

  private ColumnType parseType(String type) {
    if (DECIMAL_TYPE.matcher(type).matches()) {
      return ColumnType.DOUBLE;
    }
    switch (type) {
      case "boolean":
        return ColumnType.BOOLEAN;
      case "int":
        return ColumnType.INT;
      case "long":
        return ColumnType.LONG;
      case "float":
        return ColumnType.FLOAT;
      case "double":
        return ColumnType.DOUBLE;
      case "string":
        return ColumnType.STRING;
      case "date":
        return ColumnType.DATE;
      default:
        LOGGER.warn("unknown type hint {} in {}", type, path);
        return ColumnType.UNKNOWN;
    }
  }

  /** the first matched type of BOOLEAN, LONG, DOUBLE, DATE and STRING, empty values are skipped */
  private void inferSchema(List<CSVRecord> sample) {
    int n = names.length;
    boolean[] couldBeBoolean = new boolean[n];
    boolean[] couldBeLong = new boolean[n];
    boolean[] couldBeDouble = new boolean[n];
    boolean[] couldBeDate = new boolean[n];
    Arrays.fill(couldBeBoolean, true);
    Arrays.fill(couldBeLong, true);
    Arrays.fill(couldBeDouble, true);
    Arrays.fill(couldBeDate, true);
    boolean withTime = options.dateFormat.contains("HH") || options.dateFormat.contains("mm");

    for (CSVRecord record : sample) {
      for (int i = 0; i < Math.min(n, record.size()); i++) {
        String value = record.get(i);
        if (types[i] != ColumnType.UNKNOWN || value == null || value.trim().isEmpty()) {
          continue;
        }
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
          couldBeBoolean[i] = false;
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
            if (withTime) {
              LocalDateTime.parse(value, dateFormatter);
            } else {
              LocalDate.parse(value, dateFormatter);
            }
          } catch (DateTimeParseException e) {
            couldBeDate[i] = false;
          }
        }
      }
    }

    for (int i = 0; i < n; i++) {
      if (types[i] != ColumnType.UNKNOWN) {
        continue;
      }
      if (couldBeBoolean[i]) {
        types[i] = ColumnType.BOOLEAN;
      } else if (couldBeLong[i]) {
        types[i] = ColumnType.LONG;
      } else if (couldBeDouble[i]) {
        types[i] = ColumnType.DOUBLE;
      } else if (couldBeDate[i]) {
        types[i] = ColumnType.DATE;
      } else {
        types[i] = ColumnType.STRING;
      }
    }
  }

  private void indexColumns() throws IOException {
    Map<String, Integer> counts = new HashMap<>();
    for (String name : names) {
      if (counts.containsKey(name) && !options.allowDuplicateColumnNames) {
        throw new IOException(String.format("duplicate column name '%s' in %s", name, path));
      }
      counts.merge(name, 1, Integer::sum);
    }
    Map<String, Integer> seen = new HashMap<>();
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      int order = seen.merge(name, 1, Integer::sum);
      if (counts.get(name) > 1) {
        names[i] = name + "_" + order;
      }
      indexes.put(names[i], i);
    }
  }

  @Override
  public Map<String, DataType> find(Collection<String> patterns) {
    Map<String, DataType> result = new HashMap<>();
    for (String name : indexes.keySet()) {
      if (Patterns.match(patterns, name)) {
        result.put(name, types[indexes.get(name)].dataType);
      }
    }
    return result;
  }

  @Override
  public RowStream read(List<String> fields, RangeSet<Long> keys) throws IOException {
    int[] project =
        fields.stream()
            .map(indexes::get)
            .filter(Objects::nonNull)
            .mapToInt(Integer::intValue)
            .toArray();
    List<Field> headerFields = new ArrayList<>();
    for (int index : project) {
      headerFields.add(new Field(names[index], types[index].dataType));
    }
    return new CsvRowStream(new Header(Field.KEY, headerFields), project, keys);
  }

  private final class CsvRowStream implements RowStream {

    private final Header header;
    private final int[] project;
    private final KeyCursor cursor;
    private final BufferedReader reader;
    private final Iterator<CSVRecord> records;
    private long index = 0;
    private Row next;

    CsvRowStream(Header header, int[] project, RangeSet<Long> keys) throws IOException {
      this.header = header;
      this.project = project;
      this.cursor = new KeyCursor(keys, Long.MAX_VALUE);
      this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
      this.records = new CSVParser(reader, csvFormat).iterator();
    }

    @Nullable
    private Row fetch() {
      while (!cursor.exhausted(index) && records.hasNext()) {
        CSVRecord record = records.next();
        long key = index++;
        if (cursor.contains(key)) {
          return toRow(key, record);
        }
      }
      return null;
    }

    private Row toRow(long key, CSVRecord record) {
      Object[] values = new Object[project.length];
      for (int i = 0; i < project.length; i++) {
        int column = project[i];
        if (column < record.size()) {
          values[i] = parse(record.get(column), types[column]);
        }
      }
      return new Row(header, key, values);
    }

    @Nullable
    private Object parse(@Nullable String value, ColumnType type) {
      if (value == null) {
        return null;
      }
      try {
        switch (type) {
          case BOOLEAN:
            return Boolean.parseBoolean(value);
          case INT:
            return Integer.parseInt(value);
          case LONG:
            return Long.parseLong(value);
          case FLOAT:
            return Float.parseFloat(value);
          case DOUBLE:
            return Double.parseDouble(value);
          case DATE:
            return LocalDate.parse(value, dateFormatter)
                .atStartOfDay()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
          default:
            return value.getBytes();
        }
      } catch (NumberFormatException | DateTimeParseException e) {
        return null;
      }
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      if (next == null) {
        next = fetchOrThrow();
      }
      return next != null;
    }

    @Override
    public Row next() throws PhysicalException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row row = next;
      next = null;
      return row;
    }

    private Row fetchOrThrow() throws PhysicalException {
      try {
        return fetch();
      } catch (UncheckedIOException | IllegalStateException e) {
        throw new PhysicalException("failed to read " + path, e);
      }
    }

    @Override
    public void close() throws PhysicalException {
      try {
        reader.close();
      } catch (IOException e) {
        throw new PhysicalException(e);
      }
    }
  }
}
