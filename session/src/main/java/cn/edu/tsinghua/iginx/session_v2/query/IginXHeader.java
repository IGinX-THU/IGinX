package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Collections;
import java.util.List;

public class IginXHeader {

  public static final IginXHeader EMPTY_HEADER = new IginXHeader(Collections.emptyList());

  private final IginXColumn time;

  private final List<IginXColumn> columns;

  public IginXHeader(List<IginXColumn> columns) {
    this.time = null;
    this.columns = columns;
  }

  public IginXHeader(IginXColumn time, List<IginXColumn> columns) {
    this.time = time;
    this.columns = columns;
  }

  public List<IginXColumn> getColumns() {
    return columns;
  }

  public boolean hasTimestamp() {
    return this.time != null;
  }

  @Override
  public String toString() {
    return "IginXHeader{" + "time=" + time + ", columns=" + columns + '}';
  }
}
