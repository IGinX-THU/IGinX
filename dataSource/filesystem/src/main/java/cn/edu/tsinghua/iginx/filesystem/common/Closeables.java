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
package cn.edu.tsinghua.iginx.filesystem.common;

import java.io.Closeable;
import java.io.IOException;

public class Closeables {

  private Closeables() {}

  public static void close(Iterable<? extends Closeable> ac) throws IOException {
    if (ac == null) {
      return;
    } else if (ac instanceof Closeable) {
      ((Closeable) ac).close();
      return;
    }

    IOException exception = null;
    for (Closeable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException e) {
        if (exception == null) {
          exception = e;
        } else if (e != exception) {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  public static Closeable closeAsIOException(AutoCloseable ac) {
    return () -> {
      try {
        ac.close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    };
  }
}
