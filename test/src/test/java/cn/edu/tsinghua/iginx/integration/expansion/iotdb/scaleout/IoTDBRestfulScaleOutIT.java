package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.integration.func.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDBRestfulScaleOutIT extends RestIT {
    public IoTDBRestfulScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
        this.isAbleToDelete = false;
    }
}