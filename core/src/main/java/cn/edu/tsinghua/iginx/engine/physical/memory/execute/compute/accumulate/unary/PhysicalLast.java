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

public class PhysicalLast extends PhysicalSelect {

  public static final String NAME = "last_value";

  public PhysicalLast() {
    super(NAME);
  }

  @Override
  protected LastAccumulator<BitLastState> accumulateBit(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, BitLastState.class, BitLastState::new);
  }

  @Override
  protected LastAccumulator<IntLastState> accumulateInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, IntLastState.class, IntLastState::new);
  }

  @Override
  protected LastAccumulator<BigIntLastState> accumulateBigInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, BigIntLastState.class, BigIntLastState::new);
  }

  @Override
  protected LastAccumulator<Float4LastState> accumulateFloat4(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, Float4LastState.class, Float4LastState::new);
  }

  @Override
  protected LastAccumulator<Float8LastState> accumulateFloat8(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, Float8LastState.class, Float8LastState::new);
  }

  @Override
  protected LastAccumulator<VarBinaryLastState> accumulateVarBinary(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new LastAccumulator<>(
        allocator, inputField, outputField, VarBinaryLastState.class, VarBinaryLastState::new);
  }

  protected class LastAccumulator<S extends SelectState> extends SelectAccumulator<S> {
    protected LastAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected static class BitLastState extends BitSelectState {

    @Override
    protected boolean select(boolean newValue, boolean oldValue) {
      return oldValue;
    }
  }

  protected static class IntLastState extends IntSelectState {

    @Override
    protected int select(int newValue, int oldValue) {
      return oldValue;
    }
  }

  protected static class BigIntLastState extends BigIntSelectState {

    @Override
    protected long select(long newValue, long oldValue) {
      return oldValue;
    }
  }

  protected static class Float4LastState extends Float4SelectState {

    @Override
    protected float select(float newValue, float oldValue) {
      return oldValue;
    }
  }

  protected static class Float8LastState extends Float8SelectState {

    @Override
    protected double select(double newValue, double oldValue) {
      return oldValue;
    }
  }

  protected static class VarBinaryLastState extends VarBinarySelectState {

    @Override
    protected byte[] select(byte[] newValue, byte[] oldValue) {
      return oldValue;
    }
  }
}
