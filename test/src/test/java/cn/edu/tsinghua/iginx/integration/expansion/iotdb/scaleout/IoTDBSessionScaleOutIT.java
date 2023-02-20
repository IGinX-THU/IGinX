package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.BaseSessionIT;
import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

import java.util.LinkedHashMap;

public class IoTDBSessionScaleOutIT extends BaseSessionIT {
    public IoTDBSessionScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
    }
}
