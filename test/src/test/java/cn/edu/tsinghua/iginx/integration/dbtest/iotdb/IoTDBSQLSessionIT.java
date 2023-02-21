package cn.edu.tsinghua.iginx.integration.dbtest.iotdb;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT;

public class IoTDBSQLSessionIT extends SQLSessionIT {
    public IoTDBSQLSessionIT() {
        super();
        this.isAbleToDelete = true;
        this.isSupportSpecialPath = true;
        this.isAbleToShowTimeSeries = true;
    }
}
