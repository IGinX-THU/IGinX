package cn.edu.tsinghua.iginx.integration.expansion.iotdb.func;

import cn.edu.tsinghua.iginx.integration.func.sql.SQLSessionIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDB12SqlIT extends SQLSessionIT {
    public IoTDB12SqlIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifScaleOutIn = true;
        this.ifClearData = false;
        this.isAbleToShowTimeSeries = false;
        this.isAbleToDelete = false;
    }
}