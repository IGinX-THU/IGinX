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
package cn.edu.tsinghua.iginx.s3.strategy.impl;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.strategy.base.AbstractS3Strategy;
import javax.annotation.Nullable;

/**
 * Aliyun OSS: only accepts virtual-hosted-style requests. The SDK always sends CreateBucket and
 * GetBucketLocation in path-style, so region is required and buckets must be created in advance.
 */
public class AliyunOssStrategy extends AbstractS3Strategy {

  public AliyunOssStrategy(S3Config config) {
    super(config);
  }

  @Override
  public void validate() throws StorageInitializationException {
    super.validate();
    if (!config.hasParam(S3Config.USERNAME) || !config.hasParam(S3Config.PASSWORD)) {
      throw new StorageInitializationException(
          S3Config.USERNAME
              + " (AccessKeyId) and "
              + S3Config.PASSWORD
              + " (AccessKeySecret) are required for engine aliyun");
    }
    // bucket names are globally unique, the default one is almost certainly taken
    if (!config.isReadOnly() && !config.hasParam(S3Config.DATA_BUCKET)) {
      throw new StorageInitializationException(
          S3Config.DATA_BUCKET + " is required for writable engine aliyun");
    }
    // ListBuckets returns buckets of all regions, which cannot be accessed through this endpoint
    if (config.isHasData() && config.getDummyBucket() == null) {
      throw new StorageInitializationException(
          S3Config.DUMMY_BUCKET + " is required for engine aliyun when has_data is true");
    }
  }

  @Override
  protected String missingBucketMessage(String bucket) {
    return "bucket " + bucket + " does not exist, create it in the OSS console first";
  }

  @Override
  @Nullable
  protected String hintOf(String code) {
    switch (code) {
      case "SecondLevelDomainForbidden":
        return "OSS only accepts virtual-hosted-style requests; check the endpoint domain and the driver's bundled service configuration";
      case "SignatureDoesNotMatch":
        return "check " + S3Config.PASSWORD + " (AccessKeySecret) and " + S3Config.REGION;
      case "InvalidAccessKeyId":
        return "check " + S3Config.USERNAME + " (AccessKeyId)";
      default:
        return null;
    }
  }
}
