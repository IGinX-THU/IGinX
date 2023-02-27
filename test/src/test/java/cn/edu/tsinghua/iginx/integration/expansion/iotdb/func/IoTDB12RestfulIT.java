package cn.edu.tsinghua.iginx.integration.expansion.iotdb.func;

import cn.edu.tsinghua.iginx.integration.func.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDB12RestfulIT extends RestIT {
    public IoTDB12RestfulIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
        this.isAbleToDelete = false;
    }
}