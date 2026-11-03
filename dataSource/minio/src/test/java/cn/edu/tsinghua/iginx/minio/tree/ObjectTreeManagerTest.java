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
package cn.edu.tsinghua.iginx.minio.tree;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.minio.struct.DataTarget;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ObjectTreeManagerTest {

  private Path root;
  private ExecutorService executor;

  @Before
  public void setUp() throws IOException {
    root = Files.createTempDirectory("minio-tree-test");
    Files.write(root.resolve("a.csv"), "k,v\n1,2\n".getBytes(StandardCharsets.UTF_8));
    Files.write(root.resolve("b.parquet"), new byte[] {1, 2, 3});
    Files.createDirectories(root.resolve("sub"));
    Files.write(
        root.resolve("sub").resolve("c.txt"), "0123456789".getBytes(StandardCharsets.UTF_8));
    executor = Executors.newFixedThreadPool(2);
  }

  @After
  public void tearDown() throws IOException {
    executor.shutdownNow();
    try (Stream<Path> paths = Files.walk(root)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
        Files.delete(path);
      }
    }
  }

  private ObjectTreeConfig config() {
    Map<String, String> params = new HashMap<>();
    params.put(ObjectTreeConfig.PREFIX, "mnt");
    params.put(ObjectTreeConfig.CHUNK_SIZE_IN_BYTES, "4");
    return ObjectTreeConfig.of(ConfigFactory.parseMap(params));
  }

  private static Map<String, DataType> columns(ObjectTreeManager manager, List<String> patterns)
      throws IOException, PhysicalException {
    Map<String, DataType> columns = new TreeMap<>();
    try (RowStream stream = manager.query(new DataTarget(new BoolFilter(false), patterns, null))) {
      for (Field field : stream.getHeader().getFields()) {
        columns.put(field.getName(), field.getType());
      }
    }
    return columns;
  }

  private static Map<Long, String> rows(ObjectTreeManager manager, DataTarget target)
      throws IOException, PhysicalException {
    Map<Long, String> rows = new LinkedHashMap<>();
    try (RowStream stream = manager.query(target)) {
      assertEquals(1, stream.getHeader().getFieldSize());
      while (stream.hasNext()) {
        Row row = stream.next();
        rows.put(row.getKey(), new String((byte[]) row.getValue(0), StandardCharsets.UTF_8));
      }
    }
    return rows;
  }

  @Test
  public void testLegacyFilesystemListsEveryObjectAsBytes() throws Exception {
    ObjectTreeManager manager = ObjectTreeManager.legacyFilesystem(root, config(), executor);
    Map<String, DataType> expected = new TreeMap<>();
    expected.put("mnt.a\\.csv", DataType.BINARY);
    expected.put("mnt.b\\.parquet", DataType.BINARY);
    expected.put("mnt.sub.c\\.txt", DataType.BINARY);
    assertEquals(expected, columns(manager, null));
  }

  @Test
  public void testLegacyFilesystemEscapesNames() throws Exception {
    // backslashes are not allowed in file names on windows
    Assume.assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("win"));
    Path dir = root.resolve("d.e").resolve("p\\q");
    Files.createDirectories(dir);
    Files.write(dir.resolve("f.txt"), "x".getBytes(StandardCharsets.UTF_8));

    ObjectTreeManager legacy = ObjectTreeManager.legacyFilesystem(root, config(), executor);
    Map<String, DataType> expected =
        Collections.singletonMap("mnt.d\\.e.p\\\\q.f\\.txt", DataType.BINARY);
    assertEquals(expected, columns(legacy, Collections.singletonList("mnt.d\\.e.*")));
    assertEquals(expected, columns(legacy, Collections.singletonList("mnt.d\\.e.p\\q.*")));
    assertEquals(expected, columns(legacy, Collections.singletonList("mnt.d\\.e.p\\\\q.*")));

    ObjectTreeManager fileTree = new ObjectTreeManager(root, config(), executor);
    assertEquals(
        Collections.singletonMap("mnt.d\\.e.p\\q.f\\.txt", DataType.BINARY),
        columns(fileTree, Collections.singletonList("mnt.d\\.e.*")));
  }

  @Test
  public void testFileTreeParsesByExtension() throws Exception {
    ObjectTreeManager manager = new ObjectTreeManager(root, config(), executor);
    Map<String, DataType> columns = columns(manager, Collections.singletonList("mnt.a\\.csv.*"));
    assertEquals(new HashSet<>(Arrays.asList("mnt.a\\.csv.k", "mnt.a\\.csv.v")), columns.keySet());
  }

  @Test
  public void testLegacyFilesystemKeysAreByteOffsets() throws Exception {
    ObjectTreeManager manager = ObjectTreeManager.legacyFilesystem(root, config(), executor);
    List<String> patterns = Collections.singletonList("mnt.sub.*");

    Map<Long, String> all = rows(manager, new DataTarget(null, patterns, null));
    Map<Long, String> expected = new LinkedHashMap<>();
    expected.put(0L, "0123");
    expected.put(4L, "4567");
    expected.put(8L, "89");
    assertEquals(expected, all);

    Filter range = new AndFilter(Arrays.asList(new KeyFilter(Op.GE, 5), new KeyFilter(Op.L, 9)));
    assertEquals(
        Collections.singletonMap(5L, "5678"), rows(manager, new DataTarget(range, patterns, null)));

    Filter beyond = new KeyFilter(Op.GE, 10);
    assertTrue(rows(manager, new DataTarget(beyond, patterns, null)).isEmpty());
  }
}
