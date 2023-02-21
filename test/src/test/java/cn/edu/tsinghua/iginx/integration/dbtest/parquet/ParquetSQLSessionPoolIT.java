package cn.edu.tsinghua.iginx.integration.dbtest.parquet;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionPoolIT;

public class ParquetSQLSessionPoolIT extends SQLSessionPoolIT {

    public ParquetSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
