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
package cn.edu.tsinghua.iginx.s3.strategy.base;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.common.S3Client;
import cn.edu.tsinghua.iginx.s3.common.S3Exception;
import cn.edu.tsinghua.iginx.s3.strategy.S3Strategy;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements {@link S3Strategy} by the meta properties of the service. */
public abstract class AbstractS3Strategy implements S3Strategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractS3Strategy.class);

  private static final Set<String> NOT_FOUND_CODES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList("NoSuchBucket", "NoSuchKey", "NoSuchObject", "NoSuchVersion")));

  private static final Pattern IPV4 = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");

  protected final S3Config config;

  protected AbstractS3Strategy(S3Config config) {
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public void validate() throws StorageInitializationException {
    if (config.isRegionRequired() && config.getRegion() == null) {
      throw new StorageInitializationException(
          S3Config.REGION + " is required for engine " + config.getEngine());
    }
    if (config.isVirtualStyle()
        && (config.getEndpointHost().isEmpty() || isIpLiteral(config.getEndpointHost()))) {
      throw new StorageInitializationException(
          "virtual-hosted-style needs a domain but got "
              + config.getEndpointHost()
              + ", register the engine with the domain of the service instead of an IP");
    }
  }

  private static boolean isIpLiteral(String host) {
    return host.contains(":") || IPV4.matcher(host).matches();
  }

  @Override
  public S3Client createClient(OkHttpClient httpClient) {
    return S3Client.create(config, httpClient, this);
  }

  @Override
  public void ensureBucket(S3Client client, String bucket) throws IOException {
    if (client.bucketExists(bucket)) {
      return;
    }
    if (!config.isAutoCreateBucket()) {
      throw new IOException(missingBucketMessage(bucket));
    }
    LOGGER.info("creating bucket {} for iginx data", bucket);
    try {
      client.makeBucket(bucket);
    } catch (IOException e) {
      if (!client.bucketExists(bucket)) {
        throw e;
      }
    }
  }

  protected String missingBucketMessage(String bucket) {
    return "bucket " + bucket + " does not exist and " + S3Config.AUTO_CREATE_BUCKET + " is false";
  }

  @Override
  public IOException translate(S3Exception e) {
    String code = e.code();
    if (NOT_FOUND_CODES.contains(code)) {
      NoSuchFileException notFound = new NoSuchFileException(e.bucket() + "/" + e.key());
      notFound.initCause(e);
      return notFound;
    }
    String hint = hintOf(code);
    return new IOException(hint == null ? e.getMessage() : e.getMessage() + " (" + hint + ")", e);
  }

  /** readable hint for an error code of this service */
  @Nullable
  protected String hintOf(String code) {
    return null;
  }
}
