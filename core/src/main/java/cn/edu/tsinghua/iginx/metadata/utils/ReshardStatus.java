package cn.edu.tsinghua.iginx.metadata.utils;

public enum ReshardStatus {
  RECOVER, // 恢复阶段
  NON_RESHARDING, // 非重分片阶段
  JUDGING, // 重分片判断阶段
  EXECUTING; // 重分片执行阶段
}
