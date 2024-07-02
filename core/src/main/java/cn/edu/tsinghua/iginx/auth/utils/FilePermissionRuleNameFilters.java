package cn.edu.tsinghua.iginx.auth.utils;

import java.util.function.Predicate;

public class FilePermissionRuleNameFilters {

  public static Predicate<String> defaultRules() {
    return ruleName -> ruleName.startsWith("default");
  }

  public static Predicate<String> transformerRules() {
    return ruleName -> ruleName.startsWith("transformer");
  }

  public static Predicate<String> transformerRulesWithDefault() {
    return transformerRules().or(defaultRules());
  }

  public static Predicate<String> udfRules() {
    return ruleName -> ruleName.startsWith("udf");
  }

  public static Predicate<String> udfRulesWithDefault() {
    return udfRules().or(defaultRules());
  }

  public static Predicate<String> filesystemRules() {
    return ruleName -> ruleName.startsWith("filesystem");
  }

  public static Predicate<String> filesystemRulesWithDefault() {
    return filesystemRules().or(defaultRules());
  }
}
