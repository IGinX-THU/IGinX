/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.shared.Constant;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@AutoService(FileStructure.class)
public class LegacyFilesystem implements FileStructure {

  public static final String NAME = "LegacyFilesystem";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    return new Shared(config);
  }

  @Override
  public boolean supportFilter(FilterType type) {
    return true;
  }

  @Override
  public boolean supportAggregate(AggregateType type) {
    return false;
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
      if (config.hasPath(Constant.INIT_ROOT_PREFIX)) {
        params.put(Constant.INIT_ROOT_PREFIX, config.getString(Constant.INIT_ROOT_PREFIX));
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
      Map<String, String> finalParams = new HashMap<>(params);
      finalParams.put(Constant.INIT_INFO_DUMMY_DIR, path.toString());
      return Collections.unmodifiableMap(finalParams);
    }

    @Override
    public void close() throws IOException {}
  }
}
