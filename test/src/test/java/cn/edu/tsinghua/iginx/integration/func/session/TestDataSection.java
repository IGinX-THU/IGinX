package cn.edu.tsinghua.iginx.integration.func.session;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestDataSection {

  public static final TestDataSection EMPTY_TEST_DATA_SECTION =
      new TestDataSection(
          new ArrayList<>(),
          new ArrayList<>(),
          new ArrayList<>(),
          new ArrayList<>(),
          new ArrayList<>());

  private final List<Long> keys;

  private final List<DataType> types;

  private final List<String> paths;

  private final List<List<Object>> values;

  private final List<Map<String, String>> tagsList;

  public TestDataSection(
      List<Long> keys,
      List<DataType> types,
      List<String> paths,
      List<List<Object>> values,
      List<Map<String, String>> tagsList) {
    this.keys = keys;
    this.types = types;
    this.paths = paths;
    this.values = values;
    this.tagsList = tagsList;
  }

  public List<Long> getKeys() {
    return keys;
  }

  public List<DataType> getTypes() {
    return types;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  // return sub data section which key in [startKey, endKey)
  public TestDataSection getSubDataSectionWithKey(long startKey, long endKey) {
    List<Long> newKeys = new ArrayList<>();
    List<List<Object>> newValues = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      long key = keys.get(i);
      if (startKey <= key && key < endKey) {
        newKeys.add(key);
        newValues.add(values.get(i));
      }
    }
    return new TestDataSection(
        newKeys,
        new ArrayList<>(types),
        new ArrayList<>(paths),
        newValues,
        tagsList == null ? null : new ArrayList<>(tagsList));
  }

  public TestDataSection getSubDataSectionWithPath(List<String> selectedPaths) {
    List<Integer> indexList = new ArrayList<>();
    List<String> newPaths = new ArrayList<>();
    List<DataType> newTypes = new ArrayList<>();
    List<Map<String, String>> newTagsList = new ArrayList<>();
    for (String path : selectedPaths) {
      int index = paths.indexOf(path);
      if (index != -1) {
        indexList.add(index);
        newPaths.add(path);
        newTypes.add(types.get(index));
        newTagsList.add(tagsList.get(index));
      }
    }
    if (indexList.isEmpty()) {
      return EMPTY_TEST_DATA_SECTION;
    }

    List<List<Object>> newValues = new ArrayList<>();
    for (List<Object> rowValues : values) {
      List<Object> newRowValues = new ArrayList<>();
      for (int index : indexList) {
        newRowValues.add(rowValues.get(index));
      }
      newValues.add(newRowValues);
    }

    return new TestDataSection(new ArrayList<>(keys), newTypes, newPaths, newValues, newTagsList);
  }

  public TestDataSection mergeOther(TestDataSection other) {
    keys.addAll(other.getKeys());
    values.addAll(other.getValues());
    return this;
  }
}
