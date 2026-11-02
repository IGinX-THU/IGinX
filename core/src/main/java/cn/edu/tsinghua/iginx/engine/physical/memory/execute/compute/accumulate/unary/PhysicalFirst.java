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

public class PhysicalFirst extends PhysicalSelect {

  public static final String NAME = "first_value";

  public PhysicalFirst() {
    super(NAME);
  }

  @Override
  protected FirstAccumulator<BitFirstState> accumulateBit(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, BitFirstState.class, BitFirstState::new);
  }

  @Override
  protected FirstAccumulator<IntFirstState> accumulateInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, IntFirstState.class, IntFirstState::new);
  }

  @Override
  protected FirstAccumulator<BigIntFirstState> accumulateBigInt(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, BigIntFirstState.class, BigIntFirstState::new);
  }

  @Override
  protected FirstAccumulator<Float4FirstState> accumulateFloat4(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, Float4FirstState.class, Float4FirstState::new);
  }

  @Override
  protected FirstAccumulator<Float8FirstState> accumulateFloat8(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, Float8FirstState.class, Float8FirstState::new);
  }

  @Override
  protected FirstAccumulator<VarBinaryFirstState> accumulateVarBinary(
      BufferAllocator allocator, Field inputField, Field outputField) throws ComputeException {
    return new FirstAccumulator<>(
        allocator, inputField, outputField, VarBinaryFirstState.class, VarBinaryFirstState::new);
  }

  protected class FirstAccumulator<S extends SelectState> extends SelectAccumulator<S> {
    protected FirstAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outpuField,
        Class<S> clazz,
        Supplier<S> stateSupplier) {
      super(allocator, inputField, outpuField, clazz, stateSupplier);
    }
  }

  protected static class BitFirstState extends BitSelectState {

    @Override
    protected boolean select(boolean newValue, boolean oldValue) {
      return newValue;
    }
  }

  protected static class IntFirstState extends IntSelectState {

    @Override
    protected int select(int newValue, int oldValue) {
      return newValue;
    }
  }

  protected static class BigIntFirstState extends BigIntSelectState {

    @Override
    protected long select(long newValue, long oldValue) {
      return newValue;
    }
  }

  protected static class Float4FirstState extends Float4SelectState {

    @Override
    protected float select(float newValue, float oldValue) {
      return newValue;
    }
  }

  protected static class Float8FirstState extends Float8SelectState {

    @Override
    protected double select(double newValue, double oldValue) {
      return newValue;
    }
  }

  protected static class VarBinaryFirstState extends VarBinarySelectState {

    @Override
    protected byte[] select(byte[] newValue, byte[] oldValue) {
      return newValue;
    }
  }
}
