package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Collections;
import java.util.List;

public class IginXTable {

  public static final IginXTable EMPTY_TABLE =
      new IginXTable(IginXHeader.EMPTY_HEADER, Collections.emptyList());

  private final IginXHeader header;

  private final List<IginXRecord> records;

  public IginXTable(IginXHeader header, List<IginXRecord> records) {
    this.header = header;
    this.records = records;
  }

  public IginXHeader getHeader() {
    return header;
  }

  public List<IginXRecord> getRecords() {
    return records;
  }

  @Override
  public String toString() {
    return "IginXTable{" + "header=" + header + ", records=" + records + '}';
  }
}
