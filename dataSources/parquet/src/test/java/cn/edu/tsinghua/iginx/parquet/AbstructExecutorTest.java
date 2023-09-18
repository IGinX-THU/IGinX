package cn.edu.tsinghua.iginx.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstructExecutorTest {

  private static final Logger logger = LoggerFactory.getLogger(AbstructExecutorTest.class);

  protected static String dataDir = "dataDir/";

  protected static String dummyDir = "dummyDir/";

  protected static final String DRIVER_NAME = "org.duckdb.DuckDBDriver";

  protected static final String CONN_URL = "jdbc:duckdb:";

  protected Executor executor;

  private static long DU_INDEX = 0L;

  private static final ReentrantReadWriteLock DUIndexLock = new ReentrantReadWriteLock();

  public AbstructExecutorTest(Executor executor) {
    this.executor = executor;
  }

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

    RawData rawData = new RawData(
            pathList,
            tagsList,
            keys,
            valuesList,
            dataTypeList,
            bitmapList,
            RawDataType.Row
    );

    return new RowDataView(rawData, 0, sortedPaths.size(), 0, valuesList.length);
  }

  private String newDU() {
    try {
      DUIndexLock.writeLock().lock();
      String unit = "unit" + DU_INDEX;
      Path path = Paths.get(dataDir, unit);
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
      DU_INDEX++;
      return unit;
    } catch (Exception e) {
      logger.error("initializing new du index failed: " + e.getMessage());
      e.printStackTrace();
    } finally {
      DUIndexLock.writeLock().unlock();
    }
    return "";
  }

  @Test
  public void testEmptyInsert() {
    DataView EmptyDataView = genRowDataViewNoKey(
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new Object[0]
    );
    executor.executeInsertTask(EmptyDataView, newDU());
  }

}
