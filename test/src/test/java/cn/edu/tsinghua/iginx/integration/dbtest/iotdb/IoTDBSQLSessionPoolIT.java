package cn.edu.tsinghua.iginx.integration.dbtest.iotdb;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionPoolIT;

public class IoTDBSQLSessionPoolIT extends SQLSessionPoolIT {

    public IoTDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
