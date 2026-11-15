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
package cn.edu.tsinghua.iginx.s3.common;

import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.strategy.S3Strategy;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.ErrorResponse;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import okhttp3.OkHttpClient;

/**
 * All S3 requests of the driver go through this class, the only one that uses the MinIO SDK. Error
 * responses are translated by {@link S3Strategy#translate}; a missing bucket or object becomes
 * {@link NoSuchFileException}.
 */
public final class S3Client {

  private static final int MAX_DELETE_BATCH = 1000;

  private final MinioClient client;
  private final S3Strategy strategy;

  private S3Client(MinioClient client, S3Strategy strategy) {
    this.client = Objects.requireNonNull(client);
    this.strategy = Objects.requireNonNull(strategy);
  }

  /**
   * The SDK only uses virtual-hosted-style by default for AWS and Aliyun hosts, so the style is
   * always set explicitly from {@code virtual_style}.
   */
  public static S3Client create(S3Config config, OkHttpClient httpClient, S3Strategy strategy) {
    MinioClient.Builder builder =
        MinioClient.builder()
            .endpoint(config.getEndpointHost(), config.getEndpointPort(), config.isSecure())
            .credentials(config.getUsername(), config.getPassword())
            .httpClient(httpClient);
    if (config.getRegion() != null) {
      builder.region(config.getRegion());
    }
    MinioClient client = builder.build();
    if (config.isVirtualStyle()) {
      client.enableVirtualStyleEndpoint();
    } else {
      client.disableVirtualStyleEndpoint();
    }
    return new S3Client(client, strategy);
  }

  @FunctionalInterface
  private interface S3Callable<T> {
    T call() throws Exception;
  }

  private <T> T call(S3Callable<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (ErrorResponseException e) {
      ErrorResponse response = e.errorResponse();
      throw strategy.translate(
          new S3Exception(
              response.code(), response.bucketName(), response.objectName(), e.getMessage(), e));
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  public List<String> listBuckets() throws IOException {
    List<String> names = new ArrayList<>();
    for (Bucket bucket : call(client::listBuckets)) {
      names.add(bucket.name());
    }
    return names;
  }

  public boolean bucketExists(String bucket) throws IOException {
    return call(() -> client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build()));
  }

  public void makeBucket(String bucket) throws IOException {
    call(
        () -> {
          client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
          return null;
        });
  }

  /** all pages of the listing; without recursive, common prefixes are returned as directories */
  public List<S3Object> listObjects(String bucket, String prefix, boolean recursive)
      throws IOException {
    List<S3Object> objects = new ArrayList<>();
    for (Result<Item> result :
        client.listObjects(
            ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(recursive).build())) {
      Item item = call(result::get);
      objects.add(
          item.isDir()
              ? new S3Object(item.objectName(), 0, null, true)
              : new S3Object(
                  item.objectName(), item.size(), toInstant(item.lastModified()), false));
    }
    return objects;
  }

  /** whether any object has the prefix, only the first page of one key is requested */
  public boolean hasObjectWithPrefix(String bucket, String prefix) throws IOException {
    Iterator<Result<Item>> iterator =
        client
            .listObjects(ListObjectsArgs.builder().bucket(bucket).prefix(prefix).maxKeys(1).build())
            .iterator();
    if (!iterator.hasNext()) {
      return false;
    }
    call(iterator.next()::get);
    return true;
  }

  @Nullable
  public S3Object statObjectOrNull(String bucket, String key) throws IOException {
    StatObjectResponse stat;
    try {
      stat =
          call(
              () -> client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build()));
    } catch (NoSuchFileException e) {
      return null;
    }
    return new S3Object(key, stat.size(), toInstant(stat.lastModified()), false);
  }

  public InputStream getObject(String bucket, String key) throws IOException {
    return call(() -> client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build()));
  }

  public InputStream getObject(String bucket, String key, long offset, long length)
      throws IOException {
    return call(
        () ->
            client.getObject(
                GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .offset(offset)
                    .length(length)
                    .build()));
  }

  public void putObject(String bucket, String key, byte[] content, String contentType)
      throws IOException {
    call(
        () ->
            client.putObject(
                PutObjectArgs.builder().bucket(bucket).object(key).stream(
                        new ByteArrayInputStream(content), content.length, -1)
                    .contentType(contentType)
                    .build()));
  }

  public void removeObjects(String bucket, List<String> keys) throws IOException {
    for (int from = 0; from < keys.size(); from += MAX_DELETE_BATCH) {
      List<DeleteObject> batch = new ArrayList<>();
      for (String key : keys.subList(from, Math.min(keys.size(), from + MAX_DELETE_BATCH))) {
        batch.add(new DeleteObject(key));
      }
      Iterable<Result<DeleteError>> errors =
          client.removeObjects(RemoveObjectsArgs.builder().bucket(bucket).objects(batch).build());
      for (Result<DeleteError> error : errors) {
        DeleteError deleteError = call(error::get);
        throw new IOException(
            "failed to delete " + deleteError.objectName() + ": " + deleteError.message());
      }
    }
  }

  @Nullable
  private static Instant toInstant(@Nullable ZonedDateTime time) {
    return time == null ? null : time.toInstant();
  }
}
