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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalMax extends PhysicalSelect {

  public static final String NAME = "max";

  public PhysicalMax() {
    super(NAME);
  }

  @Override
  protected MaxAccumulator<BitMaxState> accumulateBit(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, BitMaxState.class, BitMaxState::new);
  }

  @Override
  protected MaxAccumulator<IntMaxState> accumulateInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, IntMaxState.class, IntMaxState::new);
  }

  @Override
  protected MaxAccumulator<BigIntMaxState> accumulateBigInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, BigIntMaxState.class, BigIntMaxState::new);
  }

  @Override
  protected MaxAccumulator<Float4MaxState> accumulateFloat4(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, Float4MaxState.class, Float4MaxState::new);
  }

  @Override
  protected MaxAccumulator<Float8MaxState> accumulateFloat8(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, Float8MaxState.class, Float8MaxState::new);
  }

  @Override
  protected MaxAccumulator<VarBinaryMaxState> accumulateVarBinary(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MaxAccumulator<>(
        allocator, inputField, outputField, VarBinaryMaxState.class, VarBinaryMaxState::new);
  }

  protected class MaxAccumulator<S extends SelectState> extends SelectAccumulator<S> {
    protected MaxAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected static class BitMaxState extends BitSelectState {

    @Override
    protected boolean select(boolean newValue, boolean oldValue) {
      return newValue || oldValue;
    }
  }

  protected static class IntMaxState extends IntSelectState {

    @Override
    protected int select(int newValue, int oldValue) {
      return Math.max(newValue, oldValue);
    }
  }

  protected static class BigIntMaxState extends BigIntSelectState {

    @Override
    protected long select(long newValue, long oldValue) {
      return Math.max(newValue, oldValue);
    }
  }

  protected static class Float4MaxState extends Float4SelectState {

    @Override
    protected float select(float newValue, float oldValue) {
      return Math.max(newValue, oldValue);
    }
  }

  protected static class Float8MaxState extends Float8SelectState {

    @Override
    protected double select(double newValue, double oldValue) {
      return Math.max(newValue, oldValue);
    }
  }

  protected static class VarBinaryMaxState extends VarBinarySelectState {

    @Override
    protected byte[] select(byte[] newValue, byte[] oldValue) {
      if (new String(newValue).compareTo(new String(oldValue)) > 0) {
        return newValue;
      } else {
        return oldValue;
      }
    }
  }
}
