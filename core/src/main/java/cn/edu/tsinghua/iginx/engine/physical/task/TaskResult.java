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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;

public class TaskResult<RESULT extends PhysicalCloseable> implements PhysicalCloseable {

  private PhysicalException exception;
  private RESULT result;

  public TaskResult(RESULT result) {
    this(null, Objects.requireNonNull(result));
  }

  public TaskResult(PhysicalException exception) {
    this(Objects.requireNonNull(exception), null);
  }

  private TaskResult(PhysicalException exception, RESULT result) {
    Preconditions.checkArgument(
        result == null || exception == null,
        "result and exception cannot be non-null at the same time");
    this.result = result;
    this.exception = exception;
  }

  @Override
  public void close() throws PhysicalException {
    RESULT result = unwrap();
    if (result != null) {
      result.close();
    }
  }

  @Nullable
  public RESULT unwrap() throws PhysicalException {
    if (exception != null) {
      assert result == null;
      PhysicalException exception = this.exception;
      this.exception = null;
      throw exception;
    }
    if (result != null) {
      RESULT result = this.result;
      this.result = null;
      return result;
    }
    return null;
  }
}
