package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.conf.FilePermissionConfig;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePermissionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilePermissionManager.class);

  private static final FilePermissionManager INSTANCE =
      new FilePermissionManager(FilePermissionConfig.getInstance());

  public static FilePermissionManager getInstance() {
    return INSTANCE;
  }

  private final FilePermissionConfig filePermissionConfig;
  private final AtomicReference<UserRules> rules = new AtomicReference<>(new UserRules());

  FilePermissionManager(FilePermissionConfig filePermissionConfig) {
    this.filePermissionConfig = filePermissionConfig;
    reload();
    filePermissionConfig.onReload(this::reload);
  }

  public interface Checker {
    boolean test(Path path);

    default Optional<Path> normalize(String path) {
      Optional<Path> p = cheatNormalize(Paths.get(path));
      if (!p.isPresent()) {
        return Optional.empty();
      }
      Path cheated = p.get();
      if (!test(cheated)) {
        return Optional.empty();
      }
      return Optional.of(cheated);
    }

    default boolean testRelativePath(String path) {
      return !path.contains("..");
    }

    /**
     * cheat CodeQL to not complain about path traversal vulnerability. This method should not be
     * used except you know what you are doing.
     *
     * @param path the path to normalize
     * @return the normalized path if it is safe, otherwise empty
     */
    @Deprecated
    default Optional<Path> cheatNormalize(Path path) {
      Path p = path.toAbsolutePath();
      // split path nodes
      String root = p.getRoot().toString();
      String[] nodes = new String[p.getNameCount()];
      for (int i = 0; i < p.getNameCount(); i++) {
        nodes[i] = p.getName(i).toString();
      }
      // check if any node contains "."
      if (!testRelativePath(root)) {
        return Optional.empty();
      }
      for (String node : nodes) {
        if (!testRelativePath(node)) {
          return Optional.empty();
        }
      }
      // rebuild path
      Path rebuiltPath = Paths.get(root, nodes);
      return Optional.of(rebuiltPath);
    }
  }

  public Checker getChecker(
      @Nullable String user, Predicate<String> ruleNameFilter, FileAccessType... type) {
    return path -> {
      UserRules userRules = rules.get();
      return Stream.of(type)
          .map(t -> userRules.checkPermission(user, t, ruleNameFilter, path))
          .allMatch(b -> b.orElse(true));
    };
  }

  public void reload() {
    UserRules rules = new UserRules();
    filePermissionConfig.getFilePermissions().forEach(rules::put);
    this.rules.set(rules);
    LOGGER.info("Reloaded file permissions as {}", this.rules.get());
  }

  private static class UserRules {
    private final Map<String, GroupedRuleList> rules = new HashMap<>();

    private void put(FilePermissionDescriptor descriptor) {
      GroupedRuleList groupedRuleList =
          rules.computeIfAbsent(descriptor.getUsername(), k -> new GroupedRuleList());
      groupedRuleList.put(descriptor);
    }

    Optional<Boolean> checkPermission(
        @Nullable String user, FileAccessType type, Predicate<String> ruleNameFilter, Path path) {
      GroupedRuleList specificRules = rules.get(user);
      GroupedRuleList defaultRules = rules.get(null);

      return Stream.of(specificRules, defaultRules)
          .filter(Objects::nonNull)
          .map(r -> r.checkPermission(type, ruleNameFilter, path))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst();
    }

    @Override
    public String toString() {
      return rules.toString();
    }
  }

  private static class GroupedRuleList {
    private final Map<FileAccessType, List<Rule>> rules = new HashMap<>();

    void put(FilePermissionDescriptor descriptor) {
      for (Map.Entry<FileAccessType, Boolean> entry : descriptor.getAccessMap().entrySet()) {
        List<Rule> ruleList = rules.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
        ruleList.add(new Rule(descriptor, entry.getValue()));
      }
    }

    Optional<Boolean> checkPermission(
        FileAccessType type, Predicate<String> ruleNameFilter, Path path) {
      Objects.requireNonNull(type);
      Objects.requireNonNull(ruleNameFilter);
      Objects.requireNonNull(path);

      Path absolute = path.toAbsolutePath();
      List<Rule> rules = this.rules.getOrDefault(type, Collections.emptyList());
      return rules.stream()
          .filter(rule -> ruleNameFilter.test(rule.getName()))
          .filter(rule -> rule.matches(absolute))
          .map(Rule::isAllow)
          .findFirst();
    }

    @Override
    public String toString() {
      return rules.toString();
    }
  }

  private static class Rule {
    private final FilePermissionDescriptor descriptor;
    private final boolean allow;

    Rule(FilePermissionDescriptor descriptor, boolean allow) {
      this.descriptor = Objects.requireNonNull(descriptor);
      this.allow = allow;
    }

    boolean matches(Path path) {
      return descriptor.getPathMatcher().matches(path);
    }

    String getName() {
      return descriptor.getRuleName();
    }

    boolean isAllow() {
      return allow;
    }

    @Override
    public String toString() {
      String permission = allow ? "allow" : "deny";
      return permission + "/" + descriptor.getPattern();
    }
  }
}
