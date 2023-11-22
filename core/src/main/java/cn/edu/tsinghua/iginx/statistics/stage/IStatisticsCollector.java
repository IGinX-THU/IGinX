package cn.edu.tsinghua.iginx.statistics.stage;

public interface IStatisticsCollector
    extends IParseStatisticsCollector,
        ILogicalStatisticsCollector,
        IPhysicalStatisticsCollector,
        IExecuteStatisticsCollector {

  void startBroadcasting();

  void endBroadcasting();
}
