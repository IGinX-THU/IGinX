package cn.edu.tsinghua.iginx.metadata.storage.constant;

public class Constant {

  public static final String IGINX_NODE_PREFIX = "/iginx";

  public static final String FRAGMENT_NODE_PREFIX = "/fragment";

  public static final String STORAGE_UNIT_NODE_PREFIX = "/unit";

  public static final String STORAGE_ENGINE_NODE_PREFIX = "/storage";

  public static final String STORAGE_ENGINE_LOCK_NODE = "/lock/storage";

  public static final String FRAGMENT_LOCK_NODE = "/lock/fragment";

  public static final String STORAGE_UNIT_LOCK_NODE = "/lock/unit";

  public static final String STATISTICS_FRAGMENT_POINTS_PREFIX = "/statistics/fragment/points";

  public static final String STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE =
      "/statistics/fragment/requests/write";

  public static final String STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ =
      "/statistics/fragment/requests/read";

  public static final String STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX =
      "/statistics/fragment/requests/counter";

  public static final String STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE =
      "/statistics/fragment/heat/write";

  public static final String STATISTICS_FRAGMENT_HEAT_PREFIX_READ =
      "/statistics/fragment/heat/read";

  public static final String STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX =
      "/statistics/fragment/heat/counter";

  public static final String STATISTICS_TIMESERIES_HEAT_PREFIX = "/statistics/timeseries/heat";

  public static final String STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX =
      "/statistics/timeseries/heat/counter";

  public static final String MAX_ACTIVE_END_TIME_STATISTICS_NODE =
      "/statistics/end/time/active/max/node";

  public static final String MAX_ACTIVE_END_TIME_STATISTICS_NODE_PREFIX =
      "/statistics/end/time/active/max";

  public static final String RESHARD_STATUS_NODE_PREFIX = "/status/reshard";

  public static final String RESHARD_COUNTER_NODE_PREFIX = "/counter/reshard";

  public static final String TIMESERIES_NODE_PREFIX = "/timeseries";

  public static final String TRANSFORM_NODE_PREFIX = "/transform";

  public static final String TRANSFORM_LOCK_NODE = "/lock/transform";

  public static final String USER_NODE_PREFIX = "/user";

  public static final String USER_LOCK_NODE = "/lock/user";

  public static final String SCHEMA_MAPPING_PREFIX = "/schema";

  public static final String RESHARD_STATUS_LOCK_NODE = "/lock/status/reshard";

  public static final String RESHARD_COUNTER_LOCK_NODE = "/lock/counter/reshard";

  public static final String ACTIVE_END_TIME_COUNTER_LOCK_NODE =
      "/lock/counter/end/time/active/max";

  public static final String LATENCY_COUNTER_LOCK_NODE = "/lock/counter/latency";

  public static final String FRAGMENT_HEAT_COUNTER_LOCK_NODE = "/lock/counter/fragment/heat";

  public static final String TIMESERIES_HEAT_COUNTER_LOCK_NODE = "/lock/counter/timeseries/heat";
}
