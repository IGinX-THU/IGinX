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
package cn.edu.tsinghua.iginx.minio.nio;

import static org.junit.Assert.*;

import io.minio.MinioClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class S3PathTest {

  private final S3FileSystem fs =
      new S3FileSystem(
          MinioClient.builder()
              .endpoint("http://127.0.0.1:1")
              .credentials("minioadmin", "minioadmin")
              .build(),
          Collections.emptySet(),
          1024,
          Duration.ofSeconds(1));

  @Test
  public void testBucketAndKey() {
    S3Path path = fs.getPath("/bucket", "a/b", "Iris.parquet");
    assertTrue(path.isAbsolute());
    assertEquals("bucket", path.getBucket());
    assertEquals("a/b/Iris.parquet", path.getKey());
    assertEquals("/bucket/a/b/Iris.parquet", path.toString());
    assertEquals("Iris.parquet", path.getFileName().toString());
    assertEquals(4, path.getNameCount());
    assertEquals(fs.getPath("/bucket/a/b"), path.getParent());
    assertNull(fs.root().getFileName());
  }

  @Test
  public void testResolveAndRelativize() {
    Path dir = fs.getPath("/test/a");
    Path file = dir.resolve(fs.getPath("other", "MT cars.parquet"));
    assertEquals("/test/a/other/MT cars.parquet", file.toString());
    assertEquals(fs.getPath("other/MT cars.parquet"), dir.relativize(file));
    assertTrue(file.startsWith(dir));
    assertFalse(file.startsWith(fs.getPath("/test/ab")));
    assertEquals(fs.getPath("/test/a/f"), fs.getPath("/test/a/e/../f/.").normalize());
  }

  @Test
  public void testIterateNames() {
    List<String> names = new ArrayList<>();
    for (Path name : fs.getPath("/test/a/Iris.parquet")) {
      names.add(name.toString());
    }
    assertEquals(Arrays.asList("test", "a", "Iris.parquet"), names);
  }
}
