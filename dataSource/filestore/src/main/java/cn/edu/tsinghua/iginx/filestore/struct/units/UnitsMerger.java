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
package cn.edu.tsinghua.iginx.filestore.struct.units;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeFieldRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.exception.NoSuchUnitException;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnitsMerger implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnitsMerger.class);

  public interface UnitListSupplier {
    Map<DataUnit, FileManager> getUnits() throws IOException;
  }

  private final UnitListSupplier supplier;

  public UnitsMerger(UnitListSupplier supplier) throws IOException {
    this.supplier = Objects.requireNonNull(supplier);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    long startKey = Long.MAX_VALUE;
    long endKey = Long.MIN_VALUE;
    String startColumn = "";
    String endColumn = "";
    boolean set = false;
    for (FileManager manager : supplier.getUnits().values()) {
      DataBoundary boundary = manager.getBoundary(prefix);
      if (Objects.equals(boundary, new DataBoundary())) {
        continue;
      }
      startKey = Math.min(startKey, boundary.getStartKey());
      endKey = Math.max(endKey, boundary.getEndKey());
      if (!set) {
        startColumn = boundary.getStartColumn();
        endColumn = boundary.getEndColumn();
      }
      if (startColumn != null) {
        if (boundary.getStartColumn() == null) {
          startColumn = null;
        } else if (boundary.getStartColumn().compareTo(startColumn) < 0) {
          startColumn = boundary.getStartColumn();
        }
      }
      if (endColumn != null) {
        if (boundary.getEndColumn() == null) {
          endColumn = null;
        } else if (boundary.getEndColumn().compareTo(endColumn) > 0) {
          endColumn = boundary.getEndColumn();
        }
      }
      set = true;
    }
    if (startKey >= endKey) {
      return new DataBoundary();
    }
    DataBoundary boundary = new DataBoundary(startKey, endKey);
    boundary.setStartColumn(startColumn);
    boundary.setEndColumn(endColumn);
    return boundary;
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("aggregate is not supported in UnitsMerger");
    }
    List<RowStream> streams = new ArrayList<>();
    try {
      for (Map.Entry<DataUnit, FileManager> entry : supplier.getUnits().entrySet()) {
        try {
          streams.add(entry.getValue().query(target, null));
        } catch (NoSuchUnitException e) {
          LOGGER.warn("Unit {} is not found", entry.getKey());
        }
      }
      return new MergeFieldRowStreamWrapper(streams);
    } catch (IOException | PhysicalException e) {
      IOException ex = new IOException("Failed to query data from UnitsMerger", e);
      AutoCloseables.close(ex, streams);
      throw ex;
    }
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    throw new UnsupportedOperationException("delete is not supported in UnitsMerger");
  }

  @Override
  public void insert(DataView data) throws IOException {
    throw new UnsupportedOperationException("insert is not supported in UnitsMerger");
  }

  @Override
  public void close() throws IOException {}
}
