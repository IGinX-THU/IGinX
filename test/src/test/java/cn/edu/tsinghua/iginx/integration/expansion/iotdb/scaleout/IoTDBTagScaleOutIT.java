package cn.edu.tsinghua.iginx.integration.expansion.iotdb.scaleout;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.SQLSessionIT;
import cn.edu.tsinghua.iginx.integration.TagIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

public class IoTDBTagScaleOutIT extends TagIT {

    public IoTDBTagScaleOutIT() {
        super();
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");
        TagIT.ifClearData = false;
        this.isAbleToDelete = false;
        this.ifScaleOutin = true;
    }
}
