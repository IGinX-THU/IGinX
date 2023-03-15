package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 即时对所有分片进行合并，仅用于测试
 */
public class InstantCompaction extends Compaction {

    private static final Logger logger = LoggerFactory.getLogger(InstantCompaction.class);
    private List<List<FragmentMeta>> toCompactFragmentGroups;

    public InstantCompaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
        super(physicalEngine, metaManager);
    }

    @Override
    public boolean needCompaction() {
        List<FragmentMeta> fragmentMetas = DefaultMetaManager.getInstance().getFragments();
        toCompactFragmentGroups = packFragmentsByGroup(fragmentMetas);
        return !toCompactFragmentGroups.isEmpty();
    }

    @Override
    public void compact() throws Exception {
        logger.info("start to compact all fragments");
        for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
            // 分别计算每个du的数据量，取其中数据量最多的du作为目标合并du
            StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
            compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, 0L);
        }
    }
}
