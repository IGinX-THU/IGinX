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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.metadata.entity;

import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.checkEmbeddedStorageExtraParams;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.file.Paths;
import java.util.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtraParamTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(ExtraParamTest.class);

  private static final Map<String, String> extraParam = new HashMap<>();

  private static final String IGINX_PORT = "iginx_port";

  private static final String HAS_DATA = "has_data";

  private static final String READ_ONLY = "is_read_only";

  private static final String DATA_DIR = "dir";

  private static final String DUMMY_DIR = "dummy_dir";

  // use Paths.get to avoid file seperator difference between OSes
  private static final String validDummyDir =
      Paths.get("src", "test", "resources", "data", "dummyDirParquet").toString();

  private static final String nonexistentDummyDir =
      Paths.get("src", "test", "resources", "data", "nonexistentDummyDirParquet").toString();

  private static final String validDataDir =
      Paths.get("src", "test", "resources", "data", "dataDirParquet").toString();

  private static final String nonDirFile =
      Paths.get("src", "test", "resources", "data", "nonDirFile").toString();

  private boolean isOnOs(String osName) {
    return System.getProperty("os.name").toLowerCase().contains(osName.toLowerCase());
  }

  private void initExtraParam() {
    extraParam.clear();
    extraParam.put(IGINX_PORT, "6668");
  }

  @Test
  public void validParamTest() {
    runParamTest(true, true, false, validDataDir, validDummyDir);
    runParamTest(true, false, false, validDataDir, null);
    runParamTest(true, true, true, null, validDummyDir);
  }

  @Test
  public void invalidParamTest() {
    invalidGlobalDirPathTest();

    missingDirTest();
    missingDummyDirTest();
    nonsenseParamTest();
  }

  private void runParamTest(
      boolean valid, Boolean hasData, Boolean readOnly, String dir, String dummyDir) {
    initExtraParam();
    extraParam.put(HAS_DATA, hasData.toString());
    extraParam.put(READ_ONLY, readOnly.toString());
    if (dir != null && !dir.isEmpty()) {
      extraParam.put(DATA_DIR, dir);
    }
    if (dummyDir != null && !dummyDir.isEmpty()) {
      extraParam.put(DUMMY_DIR, dummyDir);
    }
    if (valid) {
      assertTrue(checkEmbeddedStorageExtraParams(StorageEngineType.parquet, extraParam));
    } else {
      assertFalse(checkEmbeddedStorageExtraParams(StorageEngineType.parquet, extraParam));
    }
  }

  // invalid path globally
  private void invalidGlobalDirPathTest() {
    List<Pair<String, String>> pathPairList =
        new ArrayList<>(
            Arrays.asList(
                new Pair<>(nonDirFile, validDummyDir),
                new Pair<>(validDataDir, nonexistentDummyDir),
                new Pair<>(validDataDir, nonDirFile)));
    invalidPathPairsTest(pathPairList);
  }

  // invalid path only in linux/macOS, test separately because
  // will cause tests after in invalidParamTest() being ignored.
  @Test
  public void invalidLinuxDirPathTest() {
    assumeTrue(isOnOs("linux") || isOnOs("mac"));

    LOGGER.info("Testing invalid paths in linux/macOS...");

    // [<dataDir, dummyDir>...]
    List<Pair<String, String>> pathPairList =
        new ArrayList<>(
            Arrays.asList(
                new Pair<>("/", validDummyDir),
                new Pair<>("/path/to/my/data", validDummyDir),
                new Pair<>(validDataDir, "/")));
    invalidPathPairsTest(pathPairList);
  }

  // invalid path only in windows
  @Test
  public void invalidWindowsDirPathTest() {
    assumeTrue(isOnOs("win"));

    LOGGER.info("Testing invalid paths in windows...");

    // [<dataDir, dummyDir>...]
    List<Pair<String, String>> pathPairList =
        new ArrayList<>(
            Arrays.asList(
                new Pair<>("C:\\", validDummyDir),
                new Pair<>("C:/", validDummyDir),
                new Pair<>("e:\\", validDummyDir),
                new Pair<>(validDataDir, "C:\\"),
                new Pair<>(validDataDir, "C:/"),
                new Pair<>(validDataDir, "e:\\")));
    invalidPathPairsTest(pathPairList);
  }

  private void invalidPathPairsTest(List<Pair<String, String>> pathPairList) {
    for (Pair<String, String> p : pathPairList) {
      runParamTest(false, true, false, p.k, p.v);
    }
  }

  // hasData = false/true, readOnly = false, missing data dir
  private void missingDirTest() {
    runParamTest(false, true, false, null, validDummyDir);
    runParamTest(false, false, false, null, null);
  }

  // hasData = true, readOnly = false/true, missing dummy dir
  private void missingDummyDirTest() {
    runParamTest(false, true, true, null, null);
    runParamTest(false, true, false, validDataDir, null);
  }

  // hasData = false, readOnly = true, nonsense
  private void nonsenseParamTest() {
    runParamTest(false, false, true, validDataDir, validDummyDir);
  }
}
