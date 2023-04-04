package cn.edu.tsinghua.iginx.relationdb.conf;

import cn.edu.tsinghua.iginx.relationdb.conf.RelationDBConf;
public class mysqlConf extends RelationDBConf {
	
	public final boolean NEED_CHANGE_DB = true;
	public static final String DB_NAME = "mysql";
	public static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (time BIGINT NOT NULL, %s %s, PRIMARY KEY(time));";
	public static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
	public static final String CONNECTION_URL = "jdbc:mysql://%s:%s/?user=%s&password=%s";
	public static final String DEFAULT_USERNAME = "root";
	public static final String DEFAULT_PASSWORD = "";
	public static final String IGNORED_DATABASE = "sys,performance_schema,information_schema";
	public static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s";
	public static final String QUERY_DATABASES_STATEMENT = "SHOW DATABASES;";
	public static final String RELATIONDB_SEPARATOR = "\u2E82";
	public static final String DEFAULT_DATABASE = "mysql";
	public static final int BATCH_SIZE = 10000;
	public static final String CONNECTION_POOL_CLASS_NAME = "com.mysql.cj.jdbc.MysqlConnectionPoolDataSource";
}