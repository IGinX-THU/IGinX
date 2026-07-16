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
package cn.edu.tsinghua.iginx.integration.warmup;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLWarmupIT implements AutoCloseable {

  private static Logger LOGGER = LoggerFactory.getLogger(SQLWarmupIT.class);

  private static final String SQL = System.getProperty("iginx.warmup.sql", "warmup/warmup.sql");
  private static final int WARMUP = Integer.getInteger("iginx.warmup.repeat", 2);
  private static final int RECORD = Integer.getInteger("iginx.warmup.records", 50000);
  private static final String IP = System.getProperty("iginx.warmup.ip", "127.0.0.1");
  private static final int PORT = Integer.getInteger("iginx.warmup.port", 6888);
  private static final String USERNAME = System.getProperty("iginx.warmup.username", "root");
  private static final String PASSWORD = System.getProperty("iginx.warmup.password", "root");

  private final Session session;
  private final String[] queries;
  private final int records;
  private final int repeat;

  protected SQLWarmupIT(Session session, String[] queries, int records, int repeat) {
    this.session = session;
    this.records = records;
    this.queries = queries;
    this.repeat = repeat;
  }

  public SQLWarmupIT() throws IOException {
    this(new Session(IP, PORT, USERNAME, PASSWORD), getQueries(SQL), RECORD, WARMUP);
  }

  private static String[] getQueries(String name) throws IOException {
    URL url = SQLWarmupIT.class.getClassLoader().getResource(name);
    if (url == null) {
      throw new IOException("Cannot find resource " + name);
    }
    try (InputStream is = url.openStream()) {
      // read all as string
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
        return Arrays.stream(
                reader
                    .lines()
                    .filter(s -> !s.isEmpty())
                    .filter(s -> !s.startsWith("--"))
                    .collect(Collectors.joining("\n"))
                    .split(";"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s + ";")
            .toArray(String[]::new);
      }
    }
  }

  @Before
  public void setup() throws SessionException, IOException {
    session.openSession();
    session.executeSql("CLEAR DATA;");

    long[] keys = new long[records];
    Boolean[] booleanValues = new Boolean[records];
    Integer[] intValues = new Integer[records];
    Long[] longValues = new Long[records];
    Float[] floatValues = new Float[records];
    Double[] doubleValues = new Double[records];
    Object[] binaryValues = new Object[records];
    for (int i = 0; i < records; i++) {
      keys[i] = i;
      booleanValues[i] = i % 2 == 0;
      intValues[i] = i;
      longValues[i] = (long) i;
      floatValues[i] = (float) i;
      doubleValues[i] = (double) i;
      binaryValues[i] = String.valueOf(i).getBytes();
    }

    session.insertColumnRecords(
        Arrays.asList("num.i", "num.l", "num.f", "num.d"),
        keys,
        new Object[][] {intValues, longValues, floatValues, doubleValues},
        Arrays.asList(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE));

    session.insertColumnRecords(
        Arrays.asList("val.bool", "val.bin"),
        keys,
        new Object[][] {booleanValues, binaryValues},
        Arrays.asList(DataType.BOOLEAN, DataType.BINARY));
  }

  @Override
  @After
  public void close() throws Exception {
    session.executeSql("CLEAR DATA;");
    session.closeSession();
  }

  @Test
  public void warmup() {
    LOGGER.info("Start warmup with {} records", records);
    for (int i = 0; i < repeat; i++) {
      LOGGER.info("Repeat {}/{} times ", i + 1, repeat);
      for (int j = 0; j < queries.length; j++) {
        LOGGER.info("Execute query {}/{}: {}", j + 1, queries.length, queries[j]);
        try {
          session.executeSql(queries[j]);
        } catch (SessionException e) {
          LOGGER.error("Failed to execute query: {}", queries[j], e);
        }
      }
    }
  }
}
