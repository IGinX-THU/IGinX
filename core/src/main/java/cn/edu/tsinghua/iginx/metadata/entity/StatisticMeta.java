package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.Map;

public class StatisticMeta {

  private final String ip;

  private final int port;

  private final long writeCount;

  private final long writeSpan;

  private final long pointsCount;

  private final Map<OperatorType, Long> operatorCounterMap;

  private final Map<OperatorType, Long> operatorSpanMap;

  private final Map<OperatorType, Long> operatorRowsCountMap;

  public StatisticMeta(
      String ip,
      int port,
      long writeCount,
      long writeSpan,
      long pointsCount,
      Map<OperatorType, Long> operatorCounterMap,
      Map<OperatorType, Long> operatorSpanMap,
      Map<OperatorType, Long> operatorRowsCountMap) {
    this.ip = ip;
    this.port = port;
    this.writeCount = writeCount;
    this.writeSpan = writeSpan;
    this.pointsCount = pointsCount;
    this.operatorCounterMap = operatorCounterMap;
    this.operatorSpanMap = operatorSpanMap;
    this.operatorRowsCountMap = operatorRowsCountMap;
  }

  public String getIpAndPort() {
    return ip + "-" + port;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public long getWriteCount() {
    return writeCount;
  }

  public long getWriteSpan() {
    return writeSpan;
  }

  public long getPointsCount() {
    return pointsCount;
  }

  public Map<OperatorType, Long> getOperatorCounterMap() {
    return operatorCounterMap;
  }

  public Map<OperatorType, Long> getOperatorSpanMap() {
    return operatorSpanMap;
  }

  public Map<OperatorType, Long> getOperatorRowsCountMap() {
    return operatorRowsCountMap;
  }
}
