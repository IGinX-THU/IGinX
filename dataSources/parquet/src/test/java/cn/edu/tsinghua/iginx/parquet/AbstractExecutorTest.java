package cn.edu.tsinghua.iginx.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExecutorTest {

  private static final Logger logger = LoggerFactory.getLogger(AbstractExecutorTest.class);

  protected Executor executor;

  protected static int DU_INDEX = 0;

  protected static final ReentrantReadWriteLock DUIndexLock = new ReentrantReadWriteLock();

  private DataView genRowDataViewNoKey(
      List<String> pathList,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypeList,
      Object[] valuesList) {
    // sort path by dictionary
    List<String> sortedPaths = new ArrayList<>(pathList);
    Integer[] index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) valuesList[i])[index[j]];
      }
      valuesList[i] = values;
    }
    for (Integer i : index) {
      sortedDataTypeList.add(dataTypeList.get(i));
    }
    if (tagsList != null) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    // generate bitmaps and key
    List<Bitmap> bitmapList = new ArrayList<>();
    List<Long> keys = new ArrayList<>();
    long keyIndex = 0L;
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = (Object[]) valuesList[i];
      keys.set(i, keyIndex++);
      if (values.length != pathList.size()) {
        logger.error("The sizes of paths and the element of valuesList should be equal.");
        return null;
      }
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapList.add(bitmap);
    }

    RawData rawData =
        new RawData(
            sortedPaths,
            sortedTagsList,
            keys,
            valuesList,
            sortedDataTypeList,
            bitmapList,
            RawDataType.Row);

    return new RowDataView(rawData, 0, sortedPaths.size(), 0, valuesList.length);
  }

  // generate new DU and return id
  public abstract String newDU();

  @Test
  public void testEmptyInsert() {
    logger.info("Running testEmptyInsert...");
    DataView EmptyDataView =
        genRowDataViewNoKey(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new Object[0]);
    executor.executeInsertTask(EmptyDataView, newDU());
  }
}
