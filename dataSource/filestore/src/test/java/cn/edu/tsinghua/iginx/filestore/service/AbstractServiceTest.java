package cn.edu.tsinghua.iginx.filestore.service;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.test.DataViewGenerator;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractServiceTest {

  protected abstract Service getService() throws Exception;

  protected abstract DataUnit getUnit();

  private static DataView generateData(int size) {
    // construct insert statement
    List<String> pathList =
        new ArrayList<String>() {
          {
            add("us.d1.s1");
            add("us.d1.s2");
            add("us.d1.s3");
            add("us.d1.s4");
          }
        };
    List<DataType> dataTypeList =
        new ArrayList<DataType>() {
          {
            add(DataType.LONG);
            add(DataType.LONG);
            add(DataType.BINARY);
            add(DataType.DOUBLE);
          }
        };

    List<Object[]> valuesList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      valuesList.add(
          new Object[]{
              (long) i,
              (long) i + 1,
              ("\"" + RandomStringUtils.randomAlphanumeric(10) + "\"").getBytes(),
              (i + 0.1d)});
    }

    return DataViewGenerator.genRowDataViewNoKey(
        0,
        pathList,
        null,
        dataTypeList,
        valuesList
    );
  }

  private Service service;

  @BeforeEach
  public void setUp() throws Exception {
    getService().insert(getUnit(), generateData(10));
  }

  @AfterEach
  public void tearDown() throws Exception {
    getService().delete(getUnit(), new DataTarget(null, null, null));
  }

  @Test
  public void testInsertAndClear() {
  }

}
