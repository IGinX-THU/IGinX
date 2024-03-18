package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PyUDF {
  /**
   * 将位置参数转为一个2*n的列表传给python udf，包含列名和位置参数，其在列表中的位置就是它在调用udf时所在的位置
   *
   * @param posArgs 位置参数，包含列名和位置参数
   * @return 二维列表，为了传给python
   */
  public static final List<List<Object>> getPyPosParams(List<Pair<Integer, Object>> posArgs) {
    List<List<Object>> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      list.add(new ArrayList<>(Arrays.asList(p.k, p.v)));
    }
    return list;
  }
}
