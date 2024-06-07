/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.NoSuchElementException;

public class BatchPlaneScanner<K, F, V> implements Scanner<Long, Scanner<K, Scanner<F, V>>> {

  private final Scanner<K, Scanner<F, V>> planeScanner;

  private final long maxBatchSize;

  private long currentBatchSize = 0;

  private final Scanner<K, Scanner<F, V>> planeScannerHelper = new PlaneScannerHelper();

  private boolean planeScanned = false;
  private boolean lineScanned = true;

  public BatchPlaneScanner(Scanner<K, Scanner<F, V>> scannerHelper, long maxBatchSize) {
    this.planeScanner = scannerHelper;
    this.maxBatchSize = maxBatchSize;
  }

  @Override
  public Long key() {
    return currentBatchSize;
  }

  @Override
  public Scanner<K, Scanner<F, V>> value() throws NoSuchElementException {
    return planeScannerHelper;
  }

  @Override
  public boolean iterate() throws StorageException {
    if (planeScanned) {
      return false;
    }
    currentBatchSize = 0;
    return true;
  }

  private class PlaneScannerHelper implements Scanner<K, Scanner<F, V>> {

    private final Scanner<F, V> lineScannerHelper = new LineScannerHelper();

    @Override
    public K key() {
      return planeScanner.key();
    }

    @Override
    public Scanner<F, V> value() throws NoSuchElementException {
      return lineScannerHelper;
    }

    @Override
    public boolean iterate() throws StorageException {
      if (currentBatchSize >= maxBatchSize) {
        return false;
      }

      if (!lineScanned) {
        lineScanned = true;
        return true;
      }

      if (!planeScanner.iterate()) {
        planeScanned = true;
        return false;
      }
      return true;
    }

    @Override
    public void close() {}

    private class LineScannerHelper implements Scanner<F, V> {

      @Override
      public F key() throws NoSuchElementException {
        return planeScanner.value().key();
      }

      @Override
      public V value() throws NoSuchElementException {
        return planeScanner.value().value();
      }

      @Override
      public boolean iterate() throws StorageException {
        if (currentBatchSize >= maxBatchSize) {
          lineScanned = false;
          return false;
        }

        if (!planeScanner.value().iterate()) {
          return false;
        }
        currentBatchSize += SizeUtils.sizeOf(value());
        return true;
      }

      @Override
      public void close() {}
    }
  }

  @Override
  public void close() throws StorageException {
    planeScanner.close();
  }
}
