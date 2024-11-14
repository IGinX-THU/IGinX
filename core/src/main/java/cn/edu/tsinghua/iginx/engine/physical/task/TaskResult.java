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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;

public class TaskResult<RESULT extends PhysicalCloseable> implements PhysicalCloseable {

  private PhysicalException exception;
  private RESULT result;

  public TaskResult(RESULT result) {
    this(null, result);
  }

  public TaskResult(PhysicalException exception) {
    this(exception, null);
  }

  private TaskResult(PhysicalException exception, RESULT result) {
    Preconditions.checkArgument(
        result == null ^ exception == null,
        "result and exception should not both null or not null");
    this.result = result;
    this.exception = null;
  }

  @Override
  public void close() throws PhysicalException {
    RESULT result = unwrap();
    if (result != null) {
      result.close();
    }
  }

  public boolean isSuccessful() {
    return exception == null;
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
