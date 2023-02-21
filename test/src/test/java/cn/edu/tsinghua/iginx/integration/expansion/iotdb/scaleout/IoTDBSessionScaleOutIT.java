package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.integration.func.session.BaseSessionIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDBSessionScaleOutIT extends BaseSessionIT {
    public IoTDBSessionScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
    }
}
