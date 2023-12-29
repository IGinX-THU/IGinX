package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.StatisticMeta;

public interface IGinXStatisticsHook {

  void onChange(String className, StatisticMeta statisticMeta);
}
