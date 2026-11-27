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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

final class CodeTestFixture {
  static final byte[] PYTHON =
      ("class Identity:\n    def transform(self, rows):\n        return rows\n")
          .getBytes(StandardCharsets.UTF_8);
  final Path source, yaml;
  final byte[] archive;

  CodeTestFixture(SecurityTestEnvironment.Node node) throws Exception {
    source = node.home.resolve("identity.py");
    Files.write(source, PYTHON);
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    try (java.util.zip.ZipOutputStream zip = new java.util.zip.ZipOutputStream(bytes)) {
      zip.putNextEntry(new java.util.zip.ZipEntry("identity.py"));
      zip.write(PYTHON);
      zip.closeEntry();
    }
    archive = bytes.toByteArray();
    yaml = node.home.resolve("scheduled.yaml");
    Files.write(
        yaml,
        ("taskList:\n- taskType: sql\n  sqlList:\n  - 'SHOW COLUMNS;'\nexportType: none\nschedule: after 1 day\n")
            .getBytes(StandardCharsets.UTF_8));
  }

  String create(String name) {
    return "CREATE FUNCTION TRANSFORM \""
        + name
        + "\" FROM \"Identity\" IN \""
        + source.toString().replace('\\', '/')
        + "\";";
  }

  RegisterTaskReq register(long session, String name, boolean remote) {
    return new RegisterTaskReq(
        session,
        source.toString(),
        Arrays.asList(new UDFClassPair(name, "Identity")),
        Arrays.asList(UDFType.TRANSFORM),
        ByteBuffer.wrap(archive),
        remote);
  }

  CommitTransformJobReq scheduled(long session) {
    return new CommitTransformJobReq(
            session,
            Arrays.asList(
                new TaskInfo(TaskType.SQL, DataFlowType.STREAM)
                    .setSqlList(Arrays.asList("SHOW COLUMNS;"))),
            ExportType.LOG)
        .setSchedule("after 1 day");
  }

  static long submit(Connection c, CommitTransformJobReq request) throws Exception {
    request.sessionId = c.id;
    CommitTransformJobResp r = c.client.commitTransformJob(request);
    ok(r.status);
    return r.jobId;
  }

  static JobState state(Connection reader, long job) throws Exception {
    QueryTransformJobStatusResp r =
        reader.client.queryTransformJobStatus(new QueryTransformJobStatusReq(reader.id, job));
    ok(r.status);
    return r.jobState;
  }

  static void await(Connection reader, long job, JobState target) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    JobState actual;
    do {
      actual = state(reader, job);
      if (actual == target) return;
      assertFalse(
          "Job failed: " + job + " " + actual,
          actual == JobState.JOB_FAILED || actual == JobState.JOB_PARTIALLY_FAILED);
      Thread.sleep(50);
    } while (System.nanoTime() < end);
    assertEquals("Job " + job, target, actual);
  }

  static SortedMap<String, String> files(Path directory) throws Exception {
    SortedMap<String, String> result = new TreeMap<>();
    if (!Files.exists(directory)) return result;
    try (java.util.stream.Stream<Path> stream = Files.walk(directory)) {
      for (Path file : (Iterable<Path>) stream.filter(Files::isRegularFile)::iterator)
        result.put(
            directory.relativize(file).toString(),
            Base64.getEncoder().encodeToString(Files.readAllBytes(file)));
    }
    return result;
  }
}
