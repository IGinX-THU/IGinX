package cn.edu.tsinghua.iginx.integration.expansion.iotdb.func;

import cn.edu.tsinghua.iginx.integration.func.tag.TagIT;
import cn.edu.tsinghua.iginx.utils.FileReader;

public class IoTDB12TagIT extends TagIT {
    public IoTDB12TagIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        TagIT.ifClearData = false;
        this.isAbleToDelete = false;
        this.ifScaleOutin = true;
    }
}
