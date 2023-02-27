package cn.edu.tsinghua.iginx.integration.expansion.iotdb.func;

import cn.edu.tsinghua.iginx.integration.func.session.BaseSessionIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDB12SessionIT extends BaseSessionIT {
    public IoTDB12SessionIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
    }
}
