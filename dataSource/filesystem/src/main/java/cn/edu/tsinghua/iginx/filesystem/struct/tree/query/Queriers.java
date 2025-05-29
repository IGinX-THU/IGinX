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
package cn.edu.tsinghua.iginx.filesystem.struct.tree.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.RowStreams;
import cn.edu.tsinghua.iginx.filesystem.common.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Queriers {
  private Queriers() {}

  static class EmptyQuerier extends AbstractQuerier {
    @Override
    public void close() {}

    @Override
    public String toString() {
      return "EmptyQuerier{}";
    }

    @Override
    public List<Future<RowStream>> query() {
      return Collections.emptyList();
    }
  }

  private static final Querier EMPTY_QUERIER = new EmptyQuerier();

  public static Querier empty() {
    return EMPTY_QUERIER;
  }

  static class FilteredQuerier extends AbstractQuerier {
    private final Querier querier;
    private final Filter filter;

    FilteredQuerier(Querier querier, Filter filter) {
      this.querier = union(querier);
      this.filter = Objects.requireNonNull(filter);
    }

    @Override
    public void close() throws IOException {
      querier.close();
    }

    @Override
    public String toString() {
      return super.toString()
          + "&filter="
          + filter
          + "&querier="
          + Strings.shiftWithNewline(querier.toString());
    }

    @Override
    public List<Future<RowStream>> query() {
      List<Future<RowStream>> rowStreams = querier.query();
      Future<RowStream> union = union(rowStreams);
      Future<RowStream> filtered = filtered(union, filter);
      return Collections.singletonList(union);
    }
  }

  public static Future<RowStream> filtered(Future<RowStream> rowStream, Filter filter) {
    return new Future<RowStream>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return rowStream.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return rowStream.isCancelled();
      }

      @Override
      public boolean isDone() {
        return rowStream.isDone();
      }

      @Override
      public RowStream get() throws InterruptedException, ExecutionException {
        return RowStreams.filtered(rowStream.get(), filter);
      }

      @Override
      public RowStream get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return RowStreams.filtered(rowStream.get(timeout, unit), filter);
      }
    };
  }

  public static Querier filtered(Querier querier, Filter filter) {
    if (Filters.isTrue(filter)) {
      return querier;
    }
    return new FilteredQuerier(querier, filter);
  }

  static class UnionQuerier implements Querier {
    private final Querier querier;

    UnionQuerier(Querier querier) {
      this.querier = Objects.requireNonNull(querier);
    }

    @Override
    public void close() throws IOException {
      querier.close();
    }

    @Override
    public String toString() {
      return super.toString() + "&querier=" + Strings.shiftWithNewline(querier.toString());
    }

    @Override
    public List<Future<RowStream>> query() {
      List<Future<RowStream>> rowStreams = querier.query();
      Future<RowStream> result = union(rowStreams);
      return Collections.singletonList(result);
    }
  }

  public static Querier union(Querier querier) {
    return new UnionQuerier(querier);
  }

  private interface FutureGetter<T> {
    T get(Future<T> future) throws InterruptedException, ExecutionException, TimeoutException;
  }

  public static Future<RowStream> union(List<Future<RowStream>> rowStreams) {
    return new Future<RowStream>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return rowStreams.stream()
            .map(f -> f.cancel(mayInterruptIfRunning))
            .reduce((a, b) -> a && b)
            .orElse(true);
      }

      @Override
      public boolean isCancelled() {
        return rowStreams.stream().map(Future::isCancelled).reduce((a, b) -> a && b).orElse(true);
      }

      @Override
      public boolean isDone() {
        return rowStreams.stream().map(Future::isDone).reduce((a, b) -> a && b).orElse(true);
      }

      @Override
      public RowStream get() throws InterruptedException, ExecutionException {
        try {
          return getRowStream(rowStreams, Future::get);
        } catch (TimeoutException e) {
          // This should never happen as we don't use a timeout
          throw new ExecutionException(e);
        }
      }

      @Override
      public RowStream get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return getRowStream(rowStreams, future -> future.get(timeout, unit));
      }

      private RowStream getRowStream(
          List<Future<RowStream>> futures, FutureGetter<RowStream> getter)
          throws InterruptedException, ExecutionException, TimeoutException {
        List<RowStream> streams = new ArrayList<>();
        Exception exception = null;

        for (Future<RowStream> rowStream : futures) {
          try {
            streams.add(getter.get(rowStream));
          } catch (ExecutionException | InterruptedException | TimeoutException e) {
            if (exception == null) {
              exception = e;
            } else {
              exception.addSuppressed(e);
            }
          }
        }

        try {
          if (exception != null) {
            if (exception instanceof ExecutionException) {
              throw (ExecutionException) exception;
            } else if (exception instanceof InterruptedException) {
              throw (InterruptedException) exception;
            } else {
              throw (TimeoutException) exception;
            }
          }
          return RowStreams.union(streams);
        } catch (PhysicalException
            | ExecutionException
            | InterruptedException
            | TimeoutException e) {
          try {
            RowStreams.close(streams);
          } catch (PhysicalException ex) {
            e.addSuppressed(ex);
          }
          if (e instanceof PhysicalException) {
            throw new ExecutionException(e);
          } else if (e instanceof ExecutionException) {
            throw (ExecutionException) e;
          } else if (e instanceof InterruptedException) {
            throw (InterruptedException) e;
          } else {
            throw (TimeoutException) e;
          }
        }
      }
    };
  }
}
