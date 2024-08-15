/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.common.Strings;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    public List<RowStream> query() throws IOException {
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
      this.querier = merged(querier);
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
    public List<RowStream> query() throws IOException {
      List<RowStream> rowStreams = querier.query();
      assert rowStreams.size() == 1;
      return Collections.singletonList(RowStreams.filtered(rowStreams.get(0), filter));
    }
  }

  public static Querier filtered(Querier querier, Filter filter) {
    if (Filters.isTrue(filter)) {
      return querier;
    }
    return new FilteredQuerier(querier, filter);
  }

  static class MergedQuerier implements Querier {
    private final Querier querier;

    MergedQuerier(Querier querier) {
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
    public List<RowStream> query() throws IOException {
      List<RowStream> rowStreams = querier.query();
      try {
        return Collections.singletonList(RowStreams.merged(rowStreams));
      } catch (PhysicalException e) {
        Closeables.close(Iterables.transform(rowStreams, Closeables::closeAsIOException));
        throw new IOException(e);
      }
    }
  }

  public static Querier merged(Querier querier) {
    return new MergedQuerier(querier);
  }
}
