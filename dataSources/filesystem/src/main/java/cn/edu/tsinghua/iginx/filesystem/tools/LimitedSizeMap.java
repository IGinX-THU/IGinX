package cn.edu.tsinghua.iginx.filesystem.tools;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class LimitedSizeMap<K, V> extends LinkedHashMap<K, V> {
  private static final long serialVersionUID = 1L;
  private final int maxSize;
  private final Consumer<V> evictionListener;

  public LimitedSizeMap(int maxSize, Consumer<V> evictionListener) {
    super(maxSize + 1, 1.1f, true); // Ensure access order is true for LRU behavior
    this.maxSize = maxSize;
    this.evictionListener = evictionListener;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    boolean isSizeExceeded = size() > maxSize;
    if (isSizeExceeded) {
      V valueToRemove = eldest.getValue();
      if (evictionListener != null) {
        evictionListener.accept(valueToRemove);
      }
    }
    return isSizeExceeded;
  }
}
