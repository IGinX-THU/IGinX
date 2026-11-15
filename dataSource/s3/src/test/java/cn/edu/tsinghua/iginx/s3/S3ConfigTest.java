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
package cn.edu.tsinghua.iginx.s3;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class S3ConfigTest {

  private static S3Config configOf(Map<String, String> params)
      throws StorageInitializationException {
    return S3Config.of("127.0.0.1", 9000, params, false, false);
  }

  private static Map<String, String> paramsOf(String engine) {
    Map<String, String> params = new HashMap<>();
    params.put(S3Config.ENGINE, engine);
    return params;
  }

  @Test
  public void testServiceDefaults() throws Exception {
    S3Config minio = configOf(paramsOf("minio"));
    assertEquals("minio", minio.getEngine());
    assertFalse(minio.isSecure());
    assertFalse(minio.isVirtualStyle());
    assertFalse(minio.isRegionRequired());
    assertTrue(minio.isAutoCreateBucket());

    S3Config aliyun = configOf(paramsOf("Aliyun"));
    assertEquals("aliyun", aliyun.getEngine());
    assertTrue(aliyun.isSecure());
    assertTrue(aliyun.isVirtualStyle());
    assertTrue(aliyun.isRegionRequired());
    assertFalse(aliyun.isAutoCreateBucket());
  }

  @Test
  public void testExternalConfigRejected() throws Exception {
    Path directory = Files.createTempDirectory("s3-config-test");
    try {
      // A native path on every OS; rejection must not depend on filesystem error messages.
      Path file = directory.resolve("missing.properties");
      Map<String, String> params = paramsOf("minio");
      params.put("meta_properties_path", file.toString());
      assertInvalid(params, "meta_properties_path is not supported");
      Files.createFile(file);
      try {
        assertInvalid(params, "meta_properties_path is not supported");
      } finally {
        Files.delete(file);
      }
    } finally {
      Files.delete(directory);
    }
  }

  @Test
  public void testHasParam() throws Exception {
    Map<String, String> params = paramsOf("minio");
    params.put(S3Config.USERNAME, "");
    S3Config config = configOf(params);
    assertFalse(config.hasParam(S3Config.USERNAME));
    assertFalse(config.hasParam(S3Config.DATA_BUCKET));
    assertEquals(S3Config.DEFAULT_DATA_BUCKET, config.getDataBucket());
  }

  @Test
  public void testInvalidEngine() {
    assertInvalid(new HashMap<>(), "engine is required");
    assertInvalid(paramsOf("cos"), "unsupported engine: cos");
  }

  @Test
  public void testServiceCapabilitiesCannotBeOverridden() {
    for (String engine : new String[] {"minio", "aliyun"}) {
      for (String key :
          new String[] {
            "preserve_host",
            "host_policy",
            "secure",
            "virtual_style",
            "region_required",
            "auto_create_bucket",
            "PRESERVE_HOST"
          }) {
        Map<String, String> params = paramsOf(engine);
        params.put(key, "false");
        assertInvalid(params, key + " is not supported");
      }
    }
  }

  private static void assertInvalid(Map<String, String> params, String message) {
    try {
      configOf(params);
      fail();
    } catch (StorageInitializationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }
}
