package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

public class IoTDBRestfulScaleOutIT extends RestIT implements IoTDBBaseScaleOutIT{
    public IoTDBRestfulScaleOutIT() {
        super();
    }

    @Test
    public void DBConf() throws Exception {
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        this.ifClearData = false;
    }

    @Test
    public void oriHasDataExpHasDataIT() throws Exception {
        DBConf();
        capacityExpansion();
    }

    @Test
    public void oriHasDataExpNoDataIT() throws Exception {
        DBConf();
        capacityExpansion();
    }

    @Test
    public void oriNoDataExpHasDataIT() throws Exception {
        DBConf();
        capacityExpansion();
    }

    @Test
    public void oriNoDataExpNoDataIT() throws Exception {
        DBConf();
        capacityExpansion();
    }
}
