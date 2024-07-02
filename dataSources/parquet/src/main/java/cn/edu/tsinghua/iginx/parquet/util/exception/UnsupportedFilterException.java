package cn.edu.tsinghua.iginx.parquet.util.exception;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

public class UnsupportedFilterException extends StorageException {
  private final Filter filter;

  public UnsupportedFilterException(Filter filter) {
    super(String.format("unsupported filter %s", filter.toString()));
    this.filter = filter;
  }

  public UnsupportedFilterException(String message, Filter filter) {
    super(message);
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }
}
