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

public class PhysicalMin extends PhysicalSelect {

  public static final String NAME = "min";

  public PhysicalMin() {
    super(NAME);
  }

  @Override
  protected MinAccumulator<BitMinState> accumulateBit(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, BitMinState.class, BitMinState::new);
  }

  @Override
  protected MinAccumulator<IntMinState> accumulateInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, IntMinState.class, IntMinState::new);
  }

  @Override
  protected MinAccumulator<BigIntMinState> accumulateBigInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, BigIntMinState.class, BigIntMinState::new);
  }

  @Override
  protected MinAccumulator<Float4MinState> accumulateFloat4(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, Float4MinState.class, Float4MinState::new);
  }

  @Override
  protected MinAccumulator<Float8MinState> accumulateFloat8(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, Float8MinState.class, Float8MinState::new);
  }

  @Override
  protected MinAccumulator<VarBinaryMinState> accumulateVarBinary(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new MinAccumulator<>(
        allocator, inputField, outputField, VarBinaryMinState.class, VarBinaryMinState::new);
  }

  protected class MinAccumulator<S extends SelectState> extends SelectAccumulator<S> {
    protected MinAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected static class BitMinState extends BitSelectState {

    @Override
    protected boolean select(boolean newValue, boolean oldValue) {
      return newValue && oldValue;
    }
  }

  protected static class IntMinState extends IntSelectState {

    @Override
    protected int select(int newValue, int oldValue) {
      return Math.min(newValue, oldValue);
    }
  }

  protected static class BigIntMinState extends BigIntSelectState {

    @Override
    protected long select(long newValue, long oldValue) {
      return Math.min(newValue, oldValue);
    }
  }

  protected static class Float4MinState extends Float4SelectState {

    @Override
    protected float select(float newValue, float oldValue) {
      return Math.min(newValue, oldValue);
    }
  }

  protected static class Float8MinState extends Float8SelectState {

    @Override
    protected double select(double newValue, double oldValue) {
      return Math.min(newValue, oldValue);
    }
  }

  protected static class VarBinaryMinState extends VarBinarySelectState {

    @Override
    protected byte[] select(byte[] newValue, byte[] oldValue) {
      if (new String(newValue).compareTo(new String(oldValue)) < 0) {
        return newValue;
      } else {
        return oldValue;
      }
    }
  }
}
