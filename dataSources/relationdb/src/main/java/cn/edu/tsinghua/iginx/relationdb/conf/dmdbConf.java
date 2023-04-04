package cn.edu.tsinghua.iginx.relationdb.conf;

import cn.edu.tsinghua.iginx.relationdb.conf.RelationDBConf;
public class dmdbConf extends RelationDBConf {
	public final boolean NEED_CHANGE_DB = false;
	public static final String DB_NAME = "dmDB";
	public static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (time BIGINT NOT NULL, %s %s, PRIMARY KEY(time));";
	public static final String DRIVER_URL = "dm.jdbc.driver.DmDriver";
	public static final String CONNECTION_URL = "jdbc:dm://%s:%s/?user=%s&password=%s";
	public static final String DEFAULT_USERNAME = "SYSDBA";
	public static final String DEFAULT_PASSWORD = "SYSDBA001";
	public static final String IGNORED_DATABASE = "";  //dmDB中的表空间
	public static final String CREATE_DATABASE_STATEMENT = "CREATE TABLE %s (time BIGINT NOT NULL, %s %s, PRIMARY KEY(time));";
	public static final String QUERY_DATABASES_STATEMENT = null;
	public static final String RELATIONDB_SEPARATOR = ".";
	public static final String DEFAULT_DATABASE = null;
	public static final int BATCH_SIZE = 10000;
	public static final String CONNECTION_POOL_CLASS_NAME = "com.huawei.dmdb.jdbc.DmdbConnectionPoolDataSource";
}