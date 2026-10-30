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
package cn.edu.tsinghua.iginx.minio.common;

import io.minio.errors.ErrorResponseException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Converts checked exceptions of MinIO SDK into {@link IOException}. */
public final class MinioCalls {

  private static final Set<String> NOT_FOUND_CODES =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("NoSuchKey", "NoSuchObject", "NoSuchVersion")));

  private static final String NO_SUCH_BUCKET = "NoSuchBucket";

  private MinioCalls() {}

  @FunctionalInterface
  public interface MinioCallable<T> {
    T call() throws Exception;
  }

  @FunctionalInterface
  public interface MinioRunnable {
    void run() throws Exception;
  }

  public static <T> T call(MinioCallable<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (ErrorResponseException e) {
      String code = e.errorResponse().code();
      if (NO_SUCH_BUCKET.equals(code) || NOT_FOUND_CODES.contains(code)) {
        NoSuchFileException notFound =
            new NoSuchFileException(
                e.errorResponse().bucketName() + "/" + e.errorResponse().objectName());
        notFound.initCause(e);
        throw notFound;
      }
      throw new IOException(e.getMessage(), e);
    } catch (IOException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  public static void run(MinioRunnable runnable) throws IOException {
    call(
        () -> {
          runnable.run();
          return null;
        });
  }

  /** same as {@link #call(MinioCallable)}, but returns null if the object does not exist */
  public static <T> T callOrNullIfNotFound(MinioCallable<T> callable) throws IOException {
    try {
      return call(callable);
    } catch (NoSuchFileException e) {
      return null;
    }
  }
}
