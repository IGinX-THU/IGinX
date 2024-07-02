package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionManager.class);

  private static final List<Compaction> compactionList = new ArrayList<>();

  static {
    compactionList.add(
        new FragmentDeletionCompaction(
            PhysicalEngineImpl.getInstance(), DefaultMetaManager.getInstance()));
    compactionList.add(
        new LowWriteFragmentCompaction(
            PhysicalEngineImpl.getInstance(), DefaultMetaManager.getInstance()));
    compactionList.add(
        new LowAccessFragmentCompaction(
            PhysicalEngineImpl.getInstance(), DefaultMetaManager.getInstance()));
  }

  private static final CompactionManager instance = new CompactionManager();

  public static CompactionManager getInstance() {
    return instance;
  }

  public void clearFragment() throws Exception {
    if (ConfigDescriptor.getInstance().getConfig().isEnableInstantCompaction()) {
      InstantCompaction instantCompaction =
          new InstantCompaction(PhysicalEngineImpl.getInstance(), DefaultMetaManager.getInstance());
      if (instantCompaction.needCompaction()) {
        instantCompaction.compact();
      }
    } else if (ConfigDescriptor.getInstance().getConfig().isEnableFragmentCompaction()) {
      for (Compaction compaction : compactionList) {
        if (compaction.needCompaction()) {
          compaction.compact();
        }
      }
    }
  }
}
