package cn.edu.tsinghua.iginx.shared;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyClassGenerator {
  private static final Logger logger = LoggerFactory.getLogger(EmptyClassGenerator.class);

  public static DataView genRowDataViewNoKey(
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

  public static FragmentMeta genFragmentMeta() {
    return new FragmentMeta(null, null, 0, 0);
  }

  public static FragmentSource genFragmentSource() {
    return new FragmentSource(genFragmentMeta());
  }

  public static KeyInterval genKeyInterval() {
    return new KeyInterval(0, Long.MAX_VALUE);
  }

  public static DataArea genDataArea() {
    return new DataArea("unit0000000000", genKeyInterval());
  }
}
