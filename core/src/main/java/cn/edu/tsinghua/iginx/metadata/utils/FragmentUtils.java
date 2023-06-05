package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class FragmentUtils {

    public static Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>>
            keyFromTSIntervalToTimeInterval(
                    Map<ColumnsRange, List<FragmentMeta>> fragmentMapByTSInterval) {
        Map<KeyInterval, List<FragmentMeta>> fragmentMapByTimeInterval = new HashMap<>();
        List<FragmentMeta> dummyFragments = new ArrayList<>();
        fragmentMapByTSInterval.forEach(
                (k, v) -> {
                    v.forEach(
                            fragmentMeta -> {
                                if (fragmentMeta.isDummyFragment()) {
                                    dummyFragments.add(fragmentMeta);
                                    return;
                                }
                                if (fragmentMapByTimeInterval.containsKey(
                                        fragmentMeta.getKeyInterval())) {
                                    fragmentMapByTimeInterval
                                            .get(fragmentMeta.getKeyInterval())
                                            .add(fragmentMeta);
                                } else {
                                    fragmentMapByTimeInterval.put(
                                            fragmentMeta.getKeyInterval(),
                                            new ArrayList<>(
                                                    Collections.singletonList(fragmentMeta)));
                                }
                            });
                });
        return new Pair<>(fragmentMapByTimeInterval, dummyFragments);
    }
}
