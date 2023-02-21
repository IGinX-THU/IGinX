package cn.edu.tsinghua.iginx.integration.dbtest.timescaledb;

import cn.edu.tsinghua.iginx.integration.func.session.BaseSessionPoolIT;

import java.util.LinkedHashMap;

public class TimescaleDBSessionPoolIT extends BaseSessionPoolIT {

    public TimescaleDBSessionPoolIT() {
        super();
        this.defaultPort2 = 5433;
        this.isAbleToDelete = true;
        this.storageEngineType = "timescaledb";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "postgres");
        this.extraParams.put("password", "123456");
        this.extraParams.put("dbname", "timeseries");
    }
}
