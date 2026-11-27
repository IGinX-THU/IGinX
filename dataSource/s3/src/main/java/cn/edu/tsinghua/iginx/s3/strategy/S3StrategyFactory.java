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

import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.strategy.impl.AliyunOssStrategy;
import cn.edu.tsinghua.iginx.s3.strategy.impl.MinioStrategy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class S3StrategyFactory {

  public static final String MINIO = "minio";
  public static final String ALIYUN = "aliyun";

  /** every engine also has a {@code <engine>-meta.properties} in resources */
  public static final List<String> ENGINES =
      Collections.unmodifiableList(Arrays.asList(MINIO, ALIYUN));

  private S3StrategyFactory() {}

  public static S3Strategy create(S3Config config) {
    switch (config.getEngine()) {
      case MINIO:
        return new MinioStrategy(config);
      case ALIYUN:
        return new AliyunOssStrategy(config);
      default:
        throw new IllegalArgumentException(
            "unsupported engine: " + config.getEngine() + ", expect one of " + ENGINES);
    }
  }
}
