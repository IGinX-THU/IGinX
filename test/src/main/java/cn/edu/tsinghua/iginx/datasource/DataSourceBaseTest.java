package cn.edu.tsinghua.iginx.datasource;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataSourceBaseTest {

  private static final Logger logger = LoggerFactory.getLogger(DataSourceBaseTest.class);

  protected DataView genRowDataViewNoKey(
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

  public abstract void dataSourceUTTest();
}
