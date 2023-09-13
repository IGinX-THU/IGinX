package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.filesystem;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(FileSystemCapacityExpansionIT.class);

  public FileSystemCapacityExpansionIT() {
    super(filesystem, "iginx_port:6888");
    setDataTypeAndValuesForFileSystem();
  }

  private void setDataTypeAndValuesForFileSystem() {
    oriDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    expDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    readOnlyDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);

    byte[] oriValue = generateRandomValue(1);
    byte[] expValue = generateRandomValue(2);
    byte[] readOnlyValue = generateRandomValue(3);
    oriValuesList = Collections.singletonList(Arrays.asList(oriValue, oriValue));
    expValuesList = Collections.singletonList(Arrays.asList(expValue, expValue));
    expValuesList1 = Collections.singletonList(Collections.singletonList(expValue));
    expValuesList2 = Collections.singletonList(Collections.singletonList(expValue));
    readOnlyValuesList = Collections.singletonList(Arrays.asList(readOnlyValue, readOnlyValue));
  }
}
