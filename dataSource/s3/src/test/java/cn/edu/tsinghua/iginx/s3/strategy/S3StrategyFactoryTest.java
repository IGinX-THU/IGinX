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
package cn.edu.tsinghua.iginx.s3.strategy;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.strategy.impl.AliyunOssStrategy;
import cn.edu.tsinghua.iginx.s3.strategy.impl.MinioStrategy;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class S3StrategyFactoryTest {

  private static Map<String, String> aliyunParams() {
    Map<String, String> params = new HashMap<>();
    params.put(S3Config.ENGINE, "aliyun");
    params.put(S3Config.REGION, "oss-cn-hangzhou");
    params.put(S3Config.USERNAME, "ak");
    params.put(S3Config.PASSWORD, "sk");
    params.put(S3Config.DATA_BUCKET, "bkt");
    return params;
  }

  private static S3Strategy create(Map<String, String> params, boolean hasData, boolean readOnly)
      throws StorageInitializationException {
    return S3StrategyFactory.create(
        S3Config.of("oss-cn-hangzhou.aliyuncs.com", 443, params, hasData, readOnly));
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> minio = new HashMap<>();
    minio.put(S3Config.ENGINE, "minio");
    S3Strategy strategy = create(minio, false, false);
    assertTrue(strategy instanceof MinioStrategy);
    strategy.validate();

    strategy = create(aliyunParams(), false, false);
    assertTrue(strategy instanceof AliyunOssStrategy);
    strategy.validate();
  }

  @Test
  public void testAliyunValidation() throws Exception {
    Map<String, String> params = aliyunParams();
    params.remove(S3Config.REGION);
    assertInvalid(create(params, false, false), "region is required");

    params = aliyunParams();
    params.remove(S3Config.PASSWORD);
    assertInvalid(create(params, false, false), "are required for engine aliyun");

    params = aliyunParams();
    params.remove(S3Config.DATA_BUCKET);
    assertInvalid(create(params, false, false), "data_bucket is required");
    // read-only engines do not write data
    params.put(S3Config.DUMMY_BUCKET, "history");
    create(params, true, true).validate();

    assertInvalid(create(aliyunParams(), true, false), "dummy_bucket is required");
  }

  private static void assertInvalid(S3Strategy strategy, String message) {
    try {
      strategy.validate();
      fail();
    } catch (StorageInitializationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }
}
