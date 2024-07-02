package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Map;

public class IginXRecord {

  private final long key;

  private final IginXHeader header;

  private final Map<String, Object> values;

  public IginXRecord(IginXHeader header, Map<String, Object> values) {
    this.key = 0L;
    this.header = header;
    this.values = values;
  }

  public IginXRecord(long key, IginXHeader header, Map<String, Object> values) {
    this.key = key;
    this.header = header;
    this.values = values;
  }

  public IginXHeader getHeader() {
    return header;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  public Object getValue(String measurement) {
    return values.get(measurement);
  }

  public boolean hasTimestamp() {
    return header.hasTimestamp();
  }

  public long getKey() {
    return key;
  }

  @Override
  public String toString() {
    return "IginXRecord{" + "values=" + values + '}';
  }
}
