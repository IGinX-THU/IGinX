package cn.edu.tsinghua.iginx.integration.dbtest.influxdb;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionPoolIT;

public class InfluxDBSQLSessionPoolIT extends SQLSessionPoolIT {
    public InfluxDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = false;
        this.isAbleToShowTimeSeries = false;
    }
}
