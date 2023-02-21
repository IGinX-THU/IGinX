package cn.edu.tsinghua.iginx.integration.dbtest.parquet;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT;

public class ParquetSQLSessionIT extends SQLSessionIT {
    public ParquetSQLSessionIT() {
        super();
        this.isAbleToDelete = true;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = true;
    }
}
