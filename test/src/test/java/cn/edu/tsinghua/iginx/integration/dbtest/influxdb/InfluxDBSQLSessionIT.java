package cn.edu.tsinghua.iginx.integration.dbtest.influxdb;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT;

public class InfluxDBSQLSessionIT extends SQLSessionIT {
    public InfluxDBSQLSessionIT() {
        super();
        this.isAbleToDelete = false;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = false;
    }
}
