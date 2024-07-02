package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import java.util.List;

public class Migration extends AbstractUnaryOperator {

  private final FragmentMeta fragmentMeta;
  private final List<String> paths;
  private final StorageUnitMeta targetStorageUnitMeta;

  public Migration(
      GlobalSource source,
      FragmentMeta fragmentMeta,
      List<String> paths,
      StorageUnitMeta targetStorageUnitMeta) {
    super(OperatorType.Migration, source);
    this.fragmentMeta = fragmentMeta;
    this.paths = paths;
    this.targetStorageUnitMeta = targetStorageUnitMeta;
  }

  public FragmentMeta getFragmentMeta() {
    return fragmentMeta;
  }

  public StorageUnitMeta getTargetStorageUnitMeta() {
    return targetStorageUnitMeta;
  }

  public List<String> getPaths() {
    return paths;
  }

  @Override
  public Operator copy() {
    return new Migration(
        (GlobalSource) getSource().copy(), fragmentMeta, paths, targetStorageUnitMeta);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Migration((GlobalSource) source, fragmentMeta, paths, targetStorageUnitMeta);
  }

  @Override
  public String getInfo() {
    return "";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }
    Migration that = (Migration) object;
    return fragmentMeta.equals(that.fragmentMeta)
        && paths.equals(that.paths)
        && targetStorageUnitMeta.equals(that.targetStorageUnitMeta);
  }
}
