package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import java.util.*;

public abstract class Compaction {
    protected PhysicalEngine physicalEngine;
    protected IMetaManager metaManager;

    public Compaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
        this.physicalEngine = physicalEngine;
        this.metaManager = metaManager;
    }

    public abstract boolean needCompaction() throws Exception;

    public abstract void compact() throws Exception;

    protected List<List<FragmentMeta>> packFragmentsByGroup(List<FragmentMeta> fragmentMetas) {
        // 排序以减少算法时间复杂度
        fragmentMetas.sort(
                (o1, o2) -> {
                    // 先按照时间维度排序，再按照时间序列维度排序
                    if (o1.getTimeInterval().getStartTime()
                            == o2.getTimeInterval().getStartTime()) {
                        if (o1.getTsInterval().getStartTimeSeries() == null) {
                            return -1;
                        } else if (o2.getTsInterval().getStartTimeSeries() == null) {
                            return 1;
                        } else {
                            return o1.getTsInterval()
                                    .getStartTimeSeries()
                                    .compareTo(o2.getTsInterval().getStartTimeSeries());
                        }
                    } else {
                        // 所有分片在时间维度上是统一的，因此只需要根据起始时间排序即可
                        return Long.compare(
                                o1.getTimeInterval().getStartTime(),
                                o2.getTimeInterval().getStartTime());
                    }
                });

        // 对筛选出来要合并的所有分片按连通性进行分组（同一组中的分片可以合并）
        List<List<FragmentMeta>> result = new ArrayList<>();
        List<FragmentMeta> lastFragmentGroup = new ArrayList<>();
        FragmentMeta lastFragment = null;
        for (FragmentMeta fragmentMeta : fragmentMetas) {
            if (lastFragment == null) {
                lastFragmentGroup.add(fragmentMeta);
            } else {
                if (isNext(lastFragment, fragmentMeta)) {
                    lastFragmentGroup.add(fragmentMeta);
                } else {
                    if (lastFragmentGroup.size() > 1) {
                        result.add(lastFragmentGroup);
                    }
                    lastFragmentGroup = new ArrayList<>();
                }
            }
            lastFragment = fragmentMeta;
        }
        if (!lastFragmentGroup.isEmpty()) {
            result.add(lastFragmentGroup);
        }
        return result;
    }

    private boolean isNext(FragmentMeta firstFragment, FragmentMeta secondFragment) {
        if (firstFragment.getTsInterval().getEndTimeSeries() == null
                || secondFragment.getTsInterval().getStartTimeSeries() == null) {
            return false;
        } else {
            return firstFragment.getTimeInterval().equals(secondFragment.getTimeInterval())
                    && firstFragment
                            .getTsInterval()
                            .getEndTimeSeries()
                            .equals(secondFragment.getTsInterval().getStartTimeSeries());
        }
    }

    protected void compactFragmentGroupToTargetStorageUnit(
            List<FragmentMeta> fragmentGroup, StorageUnitMeta targetStorageUnit, long totalPoints)
            throws PhysicalException {
        String startTimeseries = fragmentGroup.get(0).getTsInterval().getStartTimeSeries();
        String endTimeseries = fragmentGroup.get(0).getTsInterval().getEndTimeSeries();
        long startTime = fragmentGroup.get(0).getTimeInterval().getStartTime();
        long endTime = fragmentGroup.get(0).getTimeInterval().getEndTime();

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            // 找到新分片空间
            if (startTimeseries == null
                    || fragmentMeta.getTsInterval().getStartTimeSeries() == null) {
                startTimeseries = null;
            } else {
                startTimeseries =
                        startTimeseries.compareTo(fragmentMeta.getTsInterval().getStartTimeSeries())
                                        > 0
                                ? fragmentMeta.getTsInterval().getStartTimeSeries()
                                : startTimeseries;
                if (endTimeseries == null
                        || fragmentMeta.getTsInterval().getEndTimeSeries() == null) {
                    endTimeseries = null;
                } else {
                    endTimeseries =
                            endTimeseries.compareTo(fragmentMeta.getTsInterval().getEndTimeSeries())
                                            > 0
                                    ? endTimeseries
                                    : fragmentMeta.getTsInterval().getEndTimeSeries();
                }
            }

            String storageUnitId = fragmentMeta.getMasterStorageUnitId();
            if (!storageUnitId.equals(targetStorageUnit.getId())) {
                // 重写该分片的数据
                Set<String> pathRegexSet = new HashSet<>();
                ShowTimeSeries showTimeSeries =
                        new ShowTimeSeries(
                                new GlobalSource(), pathRegexSet, null, Integer.MAX_VALUE, 0);
                RowStream rowStream = physicalEngine.execute(new RequestContext(), showTimeSeries);
                SortedSet<String> pathSet = new TreeSet<>();
                while (rowStream != null && rowStream.hasNext()) {
                    Row row = rowStream.next();
                    String timeSeries = new String((byte[]) row.getValue(0));
                    if (timeSeries.contains("{") && timeSeries.contains("}")) {
                        timeSeries = timeSeries.split("\\{")[0];
                    }
                    if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
                        pathSet.add(timeSeries);
                    }
                }
                Migration migration =
                        new Migration(
                                new GlobalSource(),
                                fragmentMeta,
                                new ArrayList<>(pathSet),
                                targetStorageUnit);
                physicalEngine.execute(new RequestContext(), migration);
            }
        }
        // TODO add write lock
        // 创建新分片
        FragmentMeta newFragment =
                new FragmentMeta(
                        startTimeseries, endTimeseries, startTime, endTime, targetStorageUnit);
        metaManager.addFragment(newFragment);
        // 更新存储点数信息
        metaManager.updateFragmentPoints(newFragment, totalPoints);

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            metaManager.removeFragment(fragmentMeta);
        }
        // TODO release write lock

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            String storageUnitId = fragmentMeta.getMasterStorageUnitId();
            if (!storageUnitId.equals(targetStorageUnit.getId())) {
                // 删除原分片节点数据
                List<String> paths = new ArrayList<>();
                paths.add(fragmentMeta.getMasterStorageUnitId() + "*");
                List<TimeRange> timeRanges = new ArrayList<>();
                timeRanges.add(
                        new TimeRange(
                                fragmentMeta.getTimeInterval().getStartTime(),
                                true,
                                fragmentMeta.getTimeInterval().getEndTime(),
                                false));
                Delete delete =
                        new Delete(new FragmentSource(fragmentMeta), timeRanges, paths, null);
                physicalEngine.execute(new RequestContext(), delete);
            }
        }
    }
}
