package cn.edu.tsinghua.iginx.engine.physical.task.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public interface PhysicalSupplier<T extends PhysicalCloseable> {
  T get() throws PhysicalException;
}
