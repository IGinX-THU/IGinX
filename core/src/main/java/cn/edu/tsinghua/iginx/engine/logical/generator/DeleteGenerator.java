package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteGenerator extends AbstractGenerator {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteGenerator.class);

  private static final DeleteGenerator instance = new DeleteGenerator();
  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();
  private final IPolicy policy =
      PolicyManager.getInstance()
          .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  private DeleteGenerator() {
    this.type = GeneratorType.Delete;
  }

  public static DeleteGenerator getInstance() {
    return instance;
  }

  @Override
  protected Operator generateRoot(Statement statement) {
    DeleteStatement deleteStatement = (DeleteStatement) statement;

    policy.notify(deleteStatement);

    List<String> pathList =
        SortUtils.mergeAndSortPaths(new ArrayList<>(deleteStatement.getPaths()));

    ColumnsInterval columnsInterval =
        new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));

    Map<ColumnsInterval, List<FragmentMeta>> fragments =
        metaManager.getFragmentMapByColumnsInterval(columnsInterval);
    if (fragments.isEmpty()) {
      if (metaManager.hasWritableStorageEngines()) {
        // on startup
        Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits =
            policy.generateInitialFragmentsAndStorageUnits(deleteStatement);
        metaManager.createInitialFragmentsAndStorageUnits(
            fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
      }
      fragments = metaManager.getFragmentMapByColumnsInterval(columnsInterval);
    }

    // 为了判定是否删除的序列中包含dummy分片的数据
    if (metaManager.hasDummyFragment(columnsInterval)) {
      deleteStatement.setInvolveDummyData(true);
    }

    TagFilter tagFilter = deleteStatement.getTagFilter();

    List<Delete> deleteList = new ArrayList<>();
    fragments.forEach(
        (k, v) ->
            v.forEach(
                fragmentMeta -> {
                  KeyInterval keyInterval = fragmentMeta.getKeyInterval();
                  if (deleteStatement.isDeleteAll()) {
                    deleteList.add(
                        new Delete(new FragmentSource(fragmentMeta), null, pathList, tagFilter));
                  } else {
                    List<KeyRange> overlapKeyRange =
                        getOverlapTimeRange(keyInterval, deleteStatement.getKeyRanges());
                    if (!overlapKeyRange.isEmpty()) {
                      deleteList.add(
                          new Delete(
                              new FragmentSource(fragmentMeta),
                              overlapKeyRange,
                              pathList,
                              tagFilter));
                    }
                  }
                }));

    List<Source> sources = new ArrayList<>();
    deleteList.forEach(operator -> sources.add(new OperatorSource(operator)));
    return new CombineNonQuery(sources);
  }

  private List<KeyRange> getOverlapTimeRange(KeyInterval interval, List<KeyRange> keyRanges) {
    List<KeyRange> res = new ArrayList<>();
    for (KeyRange range : keyRanges) {
      if (interval.getStartKey() > range.getEndKey() || interval.getEndKey() < range.getBeginKey())
        continue;
      res.add(range);
    }
    return res;
  }
}
