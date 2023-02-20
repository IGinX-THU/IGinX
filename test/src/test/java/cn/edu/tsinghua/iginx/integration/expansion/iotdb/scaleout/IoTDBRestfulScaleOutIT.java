package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

public class IoTDBRestfulScaleOutIT extends RestIT {
    public IoTDBRestfulScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
        this.isAbleToDelete = false;
    }
}