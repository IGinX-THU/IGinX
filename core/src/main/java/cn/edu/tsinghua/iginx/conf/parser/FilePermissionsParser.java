package cn.edu.tsinghua.iginx.conf.parser;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.ImmutableConfiguration;

public class FilePermissionsParser {

  private static final String INCLUDE_KEY = "include";
  private static final String DEFAULT_USERNAME = "default";

  public static List<FilePermissionDescriptor> parseList(ImmutableConfiguration config) {
    List<FilePermissionDescriptor> descriptors = new ArrayList<>();

    Map<String, LinkedHashMap<String, String>> descriptorPrefixes = parseKeys(config);
    for (Map.Entry<String, LinkedHashMap<String, String>> users : descriptorPrefixes.entrySet()) {
      String username = users.getKey();
      if (username.equals(DEFAULT_USERNAME)) {
        username = null;
      }
      LinkedHashMap<String, String> rules = users.getValue();
      for (Map.Entry<String, String> ruleArg : rules.entrySet()) {
        String ruleName = ruleArg.getKey();
        String prefix = ruleArg.getValue();
        ImmutableConfiguration ruleConfig = config.immutableSubset(prefix);
        FilePermissionDescriptor descriptor = parse(username, ruleName, ruleConfig);
        descriptors.add(descriptor);
      }
    }

    return descriptors;
  }

  private static Map<String, LinkedHashMap<String, String>> parseKeys(
      ImmutableConfiguration config) {
    Set<String> suffixes = new HashSet<>();
    for (FileAccessType accessType : FileAccessType.values()) {
      suffixes.add(accessType.name().toLowerCase());
    }

    // use `LinkedHashMap` remain the order of the keys in the configuration file
    Map<String, LinkedHashMap<String, String>> descriptorPrefixes = new HashMap<>();
    for (Iterator<String> it = config.getKeys(); it.hasNext(); ) {
      String key = it.next();

      int lastDotIndex = key.lastIndexOf('.');
      String suffix = key.substring(lastDotIndex + 1);
      if (!suffixes.contains(suffix)) {
        continue;
      }

      String prefix = key.substring(0, lastDotIndex);

      int firstDotIndex = prefix.indexOf('.');
      String username = prefix.substring(0, firstDotIndex);
      String ruleName = prefix.substring(firstDotIndex + 1);

      LinkedHashMap<String, String> ruleMap =
          descriptorPrefixes.computeIfAbsent(username, k -> new LinkedHashMap<>());
      ruleMap.putIfAbsent(ruleName, prefix);
    }
    return descriptorPrefixes;
  }

  public static FilePermissionDescriptor parse(
      @Nullable String username, String ruleName, ImmutableConfiguration config) {
    String include = config.getString(INCLUDE_KEY);
    if (include == null) {
      String message =
          String.format(
              "Rule `%s.%s` must have an include pattern",
              username == null ? DEFAULT_USERNAME : username, ruleName);
      throw new IllegalArgumentException(message);
    }
    return new FilePermissionDescriptor(
        username,
        ruleName,
        include,
        new HashMap<FileAccessType, Boolean>() {
          {
            put(
                FileAccessType.READ,
                config.getBoolean(FileAccessType.READ.name().toLowerCase(), null));
            put(
                FileAccessType.WRITE,
                config.getBoolean(FileAccessType.WRITE.name().toLowerCase(), null));
            put(
                FileAccessType.EXECUTE,
                config.getBoolean(FileAccessType.EXECUTE.name().toLowerCase(), null));
          }
        });
  }
}
