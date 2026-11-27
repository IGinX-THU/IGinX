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
package cn.edu.tsinghua.iginx.filesystem.security;

import static cn.edu.tsinghua.iginx.filesystem.security.SecurityTestEnvironment.*;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

final class StorageTestFixture {
  final SecurityTestEnvironment.Node node;
  final int port;
  final Path directory;

  StorageTestFixture(SecurityTestEnvironment.Node node) throws Exception {
    this.node = node;
    port = port();
    directory = node.home.resolve("history");
    Files.createDirectories(directory);
    Files.write(
        directory.resolve("sample.txt"), "historical-file".getBytes(StandardCharsets.UTF_8));
  }

  Map<String, String> params() {
    Map<String, String> result = new LinkedHashMap<>();
    result.put("dummy_dir", directory.toString().replace('\\', '/'));
    result.put("has_data", "true");
    result.put("is_read_only", "true");
    result.put("iginx_port", "" + node.rpc);
    result.put("schema_prefix", "history");
    return result;
  }

  StorageEngine engine() {
    return new StorageEngine("127.0.0.1", port, StorageEngineType.filesystem, params());
  }

  String addSql() {
    StringBuilder s =
        new StringBuilder(
            "ADD STORAGEENGINE (\"127.0.0.1\", " + port + ", \"filesystem\", OPTIONS (");
    for (Map.Entry<String, String> p : params().entrySet()) {
      if (s.charAt(s.length() - 1) != '(') s.append(',');
      s.append(p.getKey()).append(" '").append(p.getValue()).append("'");
    }
    return s.append("));").toString();
  }

  String removeSql() {
    return "REMOVE STORAGEENGINE (\"127.0.0.1\", " + port + ", \"history\", \"\") FOR ALL;";
  }

  RemoveStorageEngineReq remove(long id) {
    return new RemoveStorageEngineReq(
        id, Arrays.asList(new RemovedStorageEngineInfo("127.0.0.1", port, "history", "")), true);
  }

  Long id() throws Exception {
    for (StorageEngineInfo info :
        node.admin.client.getClusterInfo(new GetClusterInfoReq(node.admin.id)).storageEngineInfos)
      if (info.port == port) return info.id;
    return null;
  }

  void ensure(boolean present) throws Exception {
    Long current = id();
    if (present && current == null)
      ok(
          node.admin.client.addStorageEngines(
              new AddStorageEnginesReq(node.admin.id, Arrays.asList(engine()))));
    if (!present && current != null)
      ok(node.admin.client.removeStorageEngine(remove(node.admin.id)));
    assertEquals(present, id() != null);
  }
}
