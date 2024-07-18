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

package cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.exception.StorageException;
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
