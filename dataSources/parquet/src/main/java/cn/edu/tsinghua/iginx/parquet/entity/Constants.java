package cn.edu.tsinghua.iginx.parquet.entity;

public final class Constants {

  public static final String IGINX_SEPARATOR = "\\.";

  public static final String PARQUET_SEPARATOR = "\\$";

  public static final String SUFFIX_FILE_PARQUET = ".parquet";

  public static final String SUFFIX_FILE_TEMP = ".tmp";

  public static final String CMD_DELETE = "DELETE";

  public static final int MAX_MEM_SIZE = 1 * 1024 * 1024 /* BYTE */;

  public static final long ROW_GROUP_SIZE = 100 * 1024 /* BYTE */;

  public static final int PAGE_SIZE = 8 * 1024 /* BYTE */;

  public static final long SIZE_INSERT_BATCH = 4096;

  public static final String KEY_FIELD_NAME = "*";

  public static final String RECORD_FIELD_NAME = "*";
  public static final String DIR_DB_LSM = "lsm";
  public static final String TOMBSTONE_NAME = "iginx.tombstone";
  public static final String KEY_RANGE_NAME = "iginx.key.range";

  public static final long SEQUENCE_START = 0;
}
