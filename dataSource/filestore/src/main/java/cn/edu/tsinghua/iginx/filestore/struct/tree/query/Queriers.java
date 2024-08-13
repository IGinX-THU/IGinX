package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Queriers {
  private Queriers() {
  }

  static class EmptyQuerier implements Querier {
    @Override
    public void close() {
    }

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

  static class FilteredQuerier implements Querier {
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
      return "FilteredQuerier{" +
          "querier=" + querier +
          ", filter=" + filter +
          '}';
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
      return "MergedQuerier{" +
          "queriers=" + querier +
          '}';
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
