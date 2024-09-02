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

package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.*;
import static cn.edu.tsinghua.iginx.utils.HostUtils.isLocalHost;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class StorageEngineUtils {

  public static boolean isEmbeddedStorageEngine(StorageEngineType type) {
    return type.equals(StorageEngineType.filestore);
  }

  private static boolean isDirValid(String dir) {
    // 检查路径是否为空
    if (dir == null || dir.isEmpty()) {
      return false;
    }

    // 检查路径是否为根路径
    if (dir.equals("/") || dir.matches("[A-Za-z]:[/\\\\]")) {
      return false;
    }

    File file = new File(dir);
    return file.exists() && file.isDirectory();
  }

  public static boolean checkEmbeddedStorageExtraParams(
      StorageEngineType type, Map<String, String> extraParams) {

    if (isEmbeddedStorageEngine(type)) {
      // 必须配置iginx_port参数
      String iginxPort = extraParams.get("iginx_port");
      if (iginxPort == null || iginxPort.isEmpty()) {
        return false;
      }
      boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "false"));
      boolean readOnly =
          Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
      if (hasData) {
        // 如果hasData为true，则参数中必须配置dummy_dir
        Pair<Boolean, String> dummyDirPair = getCanonicalPath(extraParams.get("dummy_dir"));
        if (!dummyDirPair.k || !isDirValid(dummyDirPair.v)) {
          return false;
        }
        String dummyDirPath = dummyDirPair.v;
        if (!readOnly) {
          // 如果hasData为true，且readOnly为false，则参数中必须配置dir，且不能与dummy_dir相同
          Pair<Boolean, String> dirPair = getCanonicalPathWithCreate(extraParams.get("dir"));
          if (!dirPair.k || !isDirValid(dirPair.v)) {
            return false;
          }
          String dirPath = dirPair.v;
          if (dummyDirPath.equals(dirPath)) {
            return false;
          }
        }
        extraParams.put(EMBEDDED_PREFIX, StorageEngineMeta.extractEmbeddedPrefix(dummyDirPath));
      } else {
        // hasData=false readOnly=true 无意义的引擎
        if (readOnly) {
          return false;
        }
        // 如果hasData为false，则参数中必须配置dir
        Pair<Boolean, String> dirPair = getCanonicalPathWithCreate(extraParams.get("dir"));
        return dirPair.k && isDirValid(dirPair.v);
      }
    }
    return true;
  }

  public static boolean isLocal(StorageEngineMeta meta) {
    int port = ConfigDescriptor.getInstance().getConfig().getPort();
    int storageIginxPort = Integer.parseInt(meta.getExtraParams().getOrDefault("iginx_port", "-1"));
    return isLocalHost(meta.getIp()) && storageIginxPort == port;
  }

  // for dummy dir: dummy dir should exist, don't create
  private static Pair<Boolean, String> getCanonicalPath(String dir) {
    Pair<Boolean, String> invalidPair = new Pair<>(false, "");
    if (dir == null || dir.isEmpty()) { // 为空
      return invalidPair;
    }
    if (dir.equals("/") || dir.matches("[A-Za-z]:[/\\\\]")) { // 根目录
      return invalidPair;
    }
    File file = new File(dir);
    if (file.exists() && !file.isDirectory()) { // 存在但不是目录
      return invalidPair;
    }
    try {
      String canonicalPath = file.getCanonicalPath(); // 获取规范路径
      return new Pair<>(true, canonicalPath);
    } catch (IOException e) {
      return invalidPair;
    }
  }

  private static Pair<Boolean, String> getCanonicalPathWithCreate(String dir) {
    Pair<Boolean, String> invalidPair = new Pair<>(false, "");
    Pair<Boolean, String> res = getCanonicalPath(dir);
    if (res.k) { // valid path
      File file = new File(res.v);
      if (!file.exists()) { // 不存在则尝试创建
        if (!file.mkdirs()) {
          return invalidPair;
        }
      }
    }
    return res;
  }
}
