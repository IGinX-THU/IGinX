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
package cn.edu.tsinghua.iginx.filesystem.service.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.service.Service;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.test.DataValidator;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataUnit;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDummyTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDummyTest.class);

  protected final Path root;
  private final StorageConfig config;
  protected final DataUnit unit = new DataUnit(true);

  protected AbstractDummyTest(String type, Config config, String rootFileName) {
    this.root = Paths.get("target", "test", UUID.randomUUID().toString(), rootFileName);
    this.config = new StorageConfig(root.toString(), type, config);
  }

  protected Service service;

  protected DataBoundary getBoundary(@Nullable String prefix) throws FileSystemException {
    Map<DataUnit, DataBoundary> units = service.getUnits(prefix);
    LOGGER.info("units: {}", units);
    assertEquals(units.keySet(), Collections.singleton(unit));
    DataBoundary boundary = units.get(unit);
    LOGGER.info("boundary of dummy data: {}", boundary);
    return boundary;
  }

  protected Header getSchema(String... pattern) throws PhysicalException {
    return getSchema(Arrays.asList(pattern));
  }

  protected Header getSchema(List<String> pattern) throws PhysicalException {
    DataTarget target = new DataTarget(new BoolFilter(false), pattern, null);
    LOGGER.info("get schema with {}", pattern);
    try (RowStream stream = service.query(unit, target, null)) {
      Header header = stream.getHeader();
      Header sorted = DataValidator.sort(header);
      LOGGER.info("header with pattern {}: {}", pattern, sorted);
      return sorted;
    }
  }

  protected List<Row> query(List<String> pattern) throws PhysicalException {
    return query(pattern, null);
  }

  protected List<Row> query(List<String> pattern, Filter filter) throws PhysicalException {
    DataTarget target = new DataTarget(filter, pattern, null);
    LOGGER.info("query with {} and {}", pattern, filter);
    try (RowStream stream = service.query(unit, target, null)) {
      List<Row> rows = DataValidator.toList(stream);
      List<Row> normalized = DataValidator.normalize(rows);
      LOGGER.info("rows with pattern {} and filter {}: {}", pattern, filter, normalized);
      return normalized;
    }
  }

  protected static boolean isEmpty(DataBoundary boundary) {
    return Objects.equals(new DataBoundary(), boundary);
  }

  protected static boolean inBounds(DataBoundary boundary, String prefix) {
    if (isEmpty(boundary)) {
      return false;
    }
    ColumnsInterval columnsInterval =
        new ColumnsInterval(boundary.getStartColumn(), boundary.getEndColumn());
    return columnsInterval.isContain(prefix);
  }

  @BeforeEach
  public void setUp() throws IOException {
    MoreFiles.createParentDirectories(root);
  }

  protected void reset() throws IOException, FileSystemException {
    service = new StorageService(null, config);
  }

  @AfterEach
  public void tearDown() throws IOException, FileSystemException {
    if (service != null) {
      service.close();
    }
    if (Files.exists(root)) {
      MoreFiles.deleteRecursively(root, RecursiveDeleteOption.ALLOW_INSECURE);
    }
  }
}
