package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import java.util.Map;

public interface IRuleCollection {
  Map<String, Boolean> getRulesInfo();

  boolean setRules(Map<String, Boolean> rulesChange);
}
