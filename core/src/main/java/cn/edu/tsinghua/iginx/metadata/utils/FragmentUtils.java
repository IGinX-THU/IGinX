package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class FragmentUtils {

  public static Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>>
      keyFromColumnsIntervalToKeyInterval(
          Map<ColumnsInterval, List<FragmentMeta>> fragmentMapByColumnsInterval) {
    Map<KeyInterval, List<FragmentMeta>> fragmentMapByKeyInterval = new HashMap<>();
    List<FragmentMeta> dummyFragments = new ArrayList<>();
    fragmentMapByColumnsInterval.forEach(
        (k, v) ->
            v.forEach(
                fragmentMeta -> {
                  if (fragmentMeta.isDummyFragment()) {
                    dummyFragments.add(fragmentMeta);
                    return;
                  }
                  if (fragmentMapByKeyInterval.containsKey(fragmentMeta.getKeyInterval())) {
                    fragmentMapByKeyInterval.get(fragmentMeta.getKeyInterval()).add(fragmentMeta);
                  } else {
                    fragmentMapByKeyInterval.put(
                        fragmentMeta.getKeyInterval(),
                        new ArrayList<>(Collections.singletonList(fragmentMeta)));
                  }
                }));
    return new Pair<>(fragmentMapByKeyInterval, dummyFragments);
  }
}
