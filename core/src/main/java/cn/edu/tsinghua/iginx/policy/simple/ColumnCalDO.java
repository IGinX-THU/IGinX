package cn.edu.tsinghua.iginx.policy.simple;

import lombok.Data;

@Data
public class ColumnCalDO implements Comparable<ColumnCalDO> {

  private String column;

  private Long recentKey = 0L;

  private Long firstKey = Long.MAX_VALUE;

  private Long lastKey = Long.MIN_VALUE;

  private Integer count = 0;

  private Long totalByte = 0L;

  public Double getValue() {
    double ret = 0.0;
    if (count > 1 && lastKey > firstKey) {
      ret = 1.0 * totalByte / count * (count - 1) / (lastKey - firstKey);
    }
    return ret;
  }

  @Override
  public int compareTo(ColumnCalDO columnCalDO) {
    if (getValue() < columnCalDO.getValue()) {
      return -1;
    } else if (getValue() > columnCalDO.getValue()) {
      return 1;
    }
    return 0;
  }

  public void merge(Long recentKey, Long firstKey, Long lastKey, Integer count, Long totalByte) {
    this.recentKey = recentKey;
    this.firstKey = Math.min(firstKey, this.firstKey);
    this.lastKey = Math.max(lastKey, this.lastKey);
    this.count += count;
    this.totalByte += totalByte;
  }
}
