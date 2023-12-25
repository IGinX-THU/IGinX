package cn.edu.tsinghua.iginx.sql.statement;

public enum StatementType {
  NULL,
  SELECT,
  INSERT,
  DELETE,
  INSERT_FROM_SELECT,
  INSERT_FROM_CSV,
  EXPORT_CSV_FROM_SELECT,
  EXPORT_STREAM_FROM_SELECT,
  ADD_STORAGE_ENGINE,
  SHOW_REPLICATION,
  COUNT_POINTS,
  CLEAR_DATA,
  DELETE_COLUMNS,
  SHOW_COLUMNS,
  SHOW_CLUSTER_INFO,
  SHOW_REGISTER_TASK,
  REGISTER_TASK,
  DROP_TASK,
  COMMIT_TRANSFORM_JOB,
  SHOW_JOB_STATUS,
  CANCEL_JOB,
  SHOW_ELIGIBLE_JOB,
  REMOVE_HISTORY_DATA_SOURCE,
  SET_CONFIG,
  SHOW_CONFIG,
  SHOW_SESSION_ID,
  COMPACT,
  UNBAN_RULES,
  BAN_RULES,
  SHOW_RULES
}
