/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem;

import cn.edu.tsinghua.iginx.auth.FilePermissionManager;
import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.utils.FilePermissionRuleNameFilters;
import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.FileStructure;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.shared.Constant;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTreeConfig;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

@AutoService(FileStructure.class)
public class LegacyFilesystem implements FileStructure {

  public static final String NAME = "LegacyFilesystem";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    return new Shared(config);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    Shared s = (Shared) shared;
    Map<String, String> params = s.getParams(path);
    LocalExecutor executor = new LocalExecutor(true, true, params);
    return new LegacyFilesystemWrapper(executor);
  }

  @Override
  public boolean supportWrite() {
    return false;
  }

  @Override
  public FileManager newWriter(Path path, Closeable shared) throws IOException {
    throw new UnsupportedOperationException("LegacyFilesystem does not support write");
  }

  private static class Shared implements Closeable {

    private final Map<String, String> params = new HashMap<>();

    public Shared(Config config) {
      if (config.hasPath(FileTreeConfig.Fields.prefix)) {
        params.put(Constant.INIT_ROOT_PREFIX, config.getString(FileTreeConfig.Fields.prefix));
      }
      if (config.hasPath(Constant.INIT_INFO_MEMORY_POOL_SIZE)) {
        params.put(
            Constant.INIT_INFO_MEMORY_POOL_SIZE,
            config.getString(Constant.INIT_INFO_MEMORY_POOL_SIZE));
      }
      if (config.hasPath(Constant.INIT_INFO_CHUNK_SIZE)) {
        params.put(Constant.INIT_INFO_CHUNK_SIZE, config.getString(Constant.INIT_INFO_CHUNK_SIZE));
      }
    }

    public Map<String, String> getParams(Path path) {
      Path checked = check(path);
      Map<String, String> finalParams = new HashMap<>(params);
      Path absolutePath = checked.toAbsolutePath();
      finalParams.put(Constant.INIT_INFO_DUMMY_DIR, absolutePath.toString());
      return Collections.unmodifiableMap(finalParams);
    }

    private static Path check(Path path) {
      Predicate<String> ruleFilter = FilePermissionRuleNameFilters.filesystemRulesWithDefault();
      FilePermissionManager.Checker checker =
          FilePermissionManager.getInstance().getChecker(null, ruleFilter, FileAccessType.READ);

      Optional<Path> checked = checker.normalize(path.toString());
      if (!checked.isPresent()) {
        throw new SecurityException("filesystem has no permission to access: " + path);
      }
      return checked.get();
    }

    @Override
    public void close() throws IOException {}
  }
}
