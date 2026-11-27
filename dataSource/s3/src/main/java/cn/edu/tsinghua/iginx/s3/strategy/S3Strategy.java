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

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.s3.common.S3Client;
import cn.edu.tsinghua.iginx.s3.common.S3Exception;
import java.io.IOException;
import okhttp3.OkHttpClient;

/** Behaviors that differ between S3 compatible services, selected by the {@code engine} param. */
public interface S3Strategy {

  /** checks params that this service requires */
  void validate() throws StorageInitializationException;

  /** sets endpoint, credentials, region and addressing style of requests */
  S3Client createClient(OkHttpClient httpClient);

  /** makes sure the bucket of data written by IGinX exists */
  void ensureBucket(S3Client client, String bucket) throws IOException;

  /**
   * converts an error response into {@link IOException}, {@link java.nio.file.NoSuchFileException}
   * if the bucket or object does not exist
   */
  IOException translate(S3Exception e);
}
