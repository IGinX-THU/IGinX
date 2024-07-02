package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

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

  @Nonnull
  @Override
  public Long key() {
    return currentBatchSize;
  }

  @Nonnull
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

    @Nonnull
    @Override
    public K key() {
      return planeScanner.key();
    }

    @Nonnull
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

      @Nonnull
      @Override
      public F key() throws NoSuchElementException {
        return planeScanner.value().key();
      }

      @Nonnull
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
