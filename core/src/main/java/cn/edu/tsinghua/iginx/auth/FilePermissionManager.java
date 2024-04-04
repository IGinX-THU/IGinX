package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;
import cn.edu.tsinghua.iginx.conf.FilePermissionConfig;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import java.nio.file.Path;
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

  public Predicate<Path> getChecker(@Nullable String user, Module module, FileAccessType... type) {
    return path -> {
      UserRules userRules = rules.get();
      return Stream.of(type)
          .map(t -> userRules.checkPermission(user, module, path, t))
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
    private final Map<String, ModuleRules> rules = new HashMap<>();

    private void put(FilePermissionDescriptor descriptor) {
      ModuleRules moduleRules =
          rules.computeIfAbsent(descriptor.getUsername(), k -> new ModuleRules());
      moduleRules.put(descriptor);
    }

    Optional<Boolean> checkPermission(
        @Nullable String user, Module module, Path path, FileAccessType type) {
      ModuleRules specificRules = rules.get(user);
      ModuleRules defaultRules = rules.get(null);

      return Stream.of(specificRules, defaultRules)
          .filter(Objects::nonNull)
          .map(r -> r.checkPermission(module, path, type))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst();
    }

    @Override
    public String toString() {
      return rules.toString();
    }
  }

  private static class ModuleRules {
    private final Map<Module, RuleSet> rules = new HashMap<>();

    void put(FilePermissionDescriptor descriptor) {
      RuleSet ruleSet = rules.computeIfAbsent(descriptor.getModule(), k -> new RuleSet());
      ruleSet.put(descriptor);
    }

    Optional<Boolean> checkPermission(Module module, Path path, FileAccessType type) {
      Objects.requireNonNull(module);

      RuleSet ruleSet = rules.get(module);
      RuleSet defaultRuleSet = rules.get(Module.DEFAULT);

      return Stream.of(ruleSet, defaultRuleSet)
          .filter(Objects::nonNull)
          .map(s -> s.checkPermission(path, type))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst();
    }

    @Override
    public String toString() {
      return rules.toString();
    }
  }

  private static class RuleSet {
    private final Map<FileAccessType, List<Rule>> rules = new HashMap<>();

    public static final RuleSet EMPTY = new RuleSet();

    void put(FilePermissionDescriptor descriptor) {
      for (Map.Entry<FileAccessType, Boolean> entry : descriptor.getAccessMap().entrySet()) {
        List<Rule> ruleList = rules.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
        ruleList.add(new Rule(descriptor, entry.getValue()));
      }
    }

    Optional<Boolean> checkPermission(Path path, FileAccessType type) {
      Objects.requireNonNull(path);
      Objects.requireNonNull(type);

      Path absolute = path.toAbsolutePath();
      List<Rule> rules = this.rules.getOrDefault(type, Collections.emptyList());
      return rules.stream().filter(rule -> rule.matches(absolute)).map(Rule::isAllow).findFirst();
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
