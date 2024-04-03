package cn.edu.tsinghua.iginx.conf.parser;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.ImmutableConfiguration;

public class FilePermissionsParser {

  private static final String INCLUDE_KEY = "include";
  private static final String READ_KEY = "read";
  private static final String WRITE_KEY = "write";
  private static final String EXECUTE_KEY = "execute";
  private static final String DEFAULT_USERNAME = "default";
  private static final Pattern KEY_DELIMITER_PATTERN = Pattern.compile("\\.");
  private static final Pattern WITH_INDEX_PATTERN = Pattern.compile("(.+)\\[(\\d+)]");

  public static List<FilePermissionDescriptor> parseList(ImmutableConfiguration config) {
    List<FilePermissionDescriptor> descriptors = new ArrayList<>();

    Map<String, Map<Module, SortedMap<Integer, String>>> descriptorPrefixes = parseKeys(config);
    for (Map.Entry<String, Map<Module, SortedMap<Integer, String>>> entry :
        descriptorPrefixes.entrySet()) {
      String username = entry.getKey();
      if (username.equals(DEFAULT_USERNAME)) {
        username = null;
      }
      for (Map.Entry<Module, SortedMap<Integer, String>> moduleEntry :
          entry.getValue().entrySet()) {
        Module module = moduleEntry.getKey();
        for (String prefix : moduleEntry.getValue().values()) {
          ImmutableConfiguration subConfig = config.immutableSubset(prefix);
          FilePermissionDescriptor descriptor = parse(username, module, subConfig);
          descriptors.add(descriptor);
        }
      }
    }

    return descriptors;
  }

  private static Map<String, Map<Module, SortedMap<Integer, String>>> parseKeys(
      ImmutableConfiguration config) {
    Set<String> suffixes = new HashSet<>();
    for (FileAccessType accessType : FileAccessType.values()) {
      suffixes.add(accessType.name().toLowerCase());
    }

    Map<String, Map<Module, SortedMap<Integer, String>>> descriptorPrefixes = new HashMap<>();
    for (Iterator<String> it = config.getKeys(); it.hasNext(); ) {
      String key = it.next();

      int lastDotIndex = key.lastIndexOf('.');
      String suffix = key.substring(lastDotIndex + 1);
      if (!suffixes.contains(suffix)) {
        continue;
      }

      String[] keys = KEY_DELIMITER_PATTERN.split(key);
      if (keys.length < 3) {
        String msg =
            String.format("number of keys should be at least 3, but got %d: %s", keys.length, key);
        throw new IllegalArgumentException(msg);
      }

      String username = keys[0];
      String moduleNameWithIndex = keys[1];
      String prefix = String.format("%s.%s", username, moduleNameWithIndex);

      Matcher moduleNameWithIndexMatcher = WITH_INDEX_PATTERN.matcher(moduleNameWithIndex);
      if (!moduleNameWithIndexMatcher.matches()) {
        String msg =
            String.format(
                "module name should be in the format of `<name>[<number>]`, but got %s",
                moduleNameWithIndex);
        throw new IllegalArgumentException(msg);
      }

      String moduleName = moduleNameWithIndexMatcher.group(1);
      int index = Integer.parseInt(moduleNameWithIndexMatcher.group(2));

      if (!moduleName.equals(moduleName.toLowerCase())) {
        String msg = String.format("module name should be in lower case, but got %s", moduleName);
        throw new IllegalArgumentException(msg);
      }
      Module module = Module.valueOf(moduleName.toUpperCase());

      Map<Module, SortedMap<Integer, String>> moduleMap =
          descriptorPrefixes.computeIfAbsent(username, k -> new HashMap<>());
      SortedMap<Integer, String> indexMap = moduleMap.computeIfAbsent(module, k -> new TreeMap<>());
      indexMap.put(index, prefix);
    }
    return descriptorPrefixes;
  }

  public static FilePermissionDescriptor parse(
      @Nullable String username, Module module, ImmutableConfiguration config) {
    String include = config.getString(INCLUDE_KEY);
    return new FilePermissionDescriptor(
        username,
        module,
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
