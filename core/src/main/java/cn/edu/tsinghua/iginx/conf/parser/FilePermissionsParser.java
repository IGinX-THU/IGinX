package cn.edu.tsinghua.iginx.conf.parser;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import org.apache.commons.configuration2.ImmutableConfiguration;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilePermissionsParser {

  private static final String INCLUDE_KEY = "include";
  private static final String READ_KEY = "read";
  private static final String WRITE_KEY = "write";
  private static final String EXECUTE_KEY = "execute";
  private static final Pattern KEY_DELIMITER_PATTERN = Pattern.compile("\\.");
  private static final Pattern NAME_WITH_NUMBER_PATTERN = Pattern.compile("(.+)\\[(\\d+)]");

  public static List<FilePermissionDescriptor> parseList(ImmutableConfiguration config) {
    List<FilePermissionDescriptor> descriptors = new ArrayList<>();

    Map<String, SortedMap<Integer, Map<Module, String>>> descriptorPrefixes = parseKeys(config);
    for (Map.Entry<String, SortedMap<Integer, Map<Module, String>>> entry : descriptorPrefixes.entrySet()) {
      String username = entry.getKey();
      for (Map<Module, String> moduleEntry : entry.getValue().values()) {
        for (Map.Entry<Module, String> prefixEntry : moduleEntry.entrySet()) {
          Module module = prefixEntry.getKey();
          String prefix = prefixEntry.getValue();
          ImmutableConfiguration subConfig = config.immutableSubset(prefix);
          FilePermissionDescriptor descriptor = parse(username, module, subConfig);
          descriptors.add(descriptor);
        }
      }
    }

    return descriptors;
  }

  private static Map<String, SortedMap<Integer, Map<Module, String>>> parseKeys(ImmutableConfiguration config) {
    Map<String, SortedMap<Integer, Map<Module, String>>> descriptorPrefixes = new HashMap<>();
    for (Iterator<String> it = config.getKeys(); it.hasNext(); ) {
      String key = it.next();
      String[] keys = KEY_DELIMITER_PATTERN.split(key);
      if (keys.length < 3) {
        String msg = String.format("number of keys should be at least 3, but got %d: %s", keys.length, key);
        throw new IllegalArgumentException(msg);
      }

      String usernameWithNumber = keys[0];
      String moduleName = keys[1];
      String prefix = String.format("%s.%s", usernameWithNumber, moduleName);

      Matcher nameWithNumberMatcher = NAME_WITH_NUMBER_PATTERN.matcher(usernameWithNumber);
      if (!nameWithNumberMatcher.matches()) {
        String msg = String.format("name should be in the format of `<name>[<number>]`, but got %s", usernameWithNumber);
        throw new IllegalArgumentException(msg);
      }

      String username = nameWithNumberMatcher.group(1);
      int number = Integer.parseInt(nameWithNumberMatcher.group(2));

      if (!moduleName.equals(moduleName.toLowerCase())) {
        String msg = String.format("module should be in lower case, but got %s", moduleName);
        throw new IllegalArgumentException(msg);
      }
      Module module = Module.valueOf(moduleName.toUpperCase());

      SortedMap<Integer, Map<Module, String>> numberMap = descriptorPrefixes.computeIfAbsent(username, k -> new TreeMap<>());
      Map<Module, String> moduleMap = numberMap.computeIfAbsent(number, k -> new HashMap<>());
      moduleMap.put(module, prefix);
    }
    return descriptorPrefixes;
  }

  public static FilePermissionDescriptor parse(@Nullable String username, @Nullable Module module, ImmutableConfiguration config) {
    String include = config.getString(INCLUDE_KEY);
    return new FilePermissionDescriptor(username, module, include, new HashMap<FileAccessType, Boolean>() {{
      put(FileAccessType.READ, config.getBoolean(READ_KEY));
      put(FileAccessType.WRITE, config.getBoolean(WRITE_KEY));
      put(FileAccessType.EXECUTE, config.getBoolean(EXECUTE_KEY));
    }});
  }
}
