package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDBSqlScaleOutIT extends SQLSessionIT {
    public IoTDBSqlScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifScaleOutIn = true;
        this.ifClearData = false;
        this.isAbleToShowTimeSeries = false;
        this.isAbleToDelete = false;
    }
}