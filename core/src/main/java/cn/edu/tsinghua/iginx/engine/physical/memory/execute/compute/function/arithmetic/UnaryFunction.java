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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public abstract class UnaryFunction extends AbstractFunction {

  public UnaryFunction(String functionName) {
    super(functionName, Arity.UNARY);
  }

  public abstract int evaluate(int value);

  public abstract long evaluate(long value);

  public abstract float evaluate(float value);

  public abstract double evaluate(double value);

  @Override
  protected VectorSchemaRoot invokeImpl(
      ExecutorContext context, @WillNotClose VectorSchemaRoot args) {
    FieldVector vector = args.getVector(0);
    FieldVector resultVector = invokeImpl(context, vector);
    return new VectorSchemaRoot(Collections.singleton(resultVector));
  }

  protected FieldVector invokeImpl(ExecutorContext context, FieldVector vector) {
    if (vector instanceof NullVector) {
      return null;
    }
    ArrowBuf buf = vector.getDataBuffer();
    int rowCount = vector.getValueCount();
    switch (vector.getMinorType()) {
      case INT:
        {
          IntVector dest =
              (IntVector) ValueVectors.create(context.getAllocator(), Types.MinorType.INT);
          ArrowBuf destBuf = dest.getDataBuffer();
          for (long i = 0; i < rowCount; i++) {
            long index = i * IntVector.TYPE_WIDTH;
            destBuf.setInt(index, evaluate(buf.getInt(index)));
          }
          return dest;
        }
      case BIGINT:
        {
          BigIntVector dest =
              (BigIntVector) ValueVectors.create(context.getAllocator(), Types.MinorType.BIGINT);
          ArrowBuf destBuf = dest.getDataBuffer();
          for (long i = 0; i < rowCount; i++) {
            long index = i * BigIntVector.TYPE_WIDTH;
            destBuf.setLong(index, evaluate(buf.getLong(index)));
          }
          return dest;
        }
      case FLOAT4:
        {
          Float4Vector dest =
              (Float4Vector) ValueVectors.create(context.getAllocator(), Types.MinorType.FLOAT4);
          ArrowBuf destBuf = dest.getDataBuffer();
          for (long i = 0; i < rowCount; i++) {
            long index = i * Float4Vector.TYPE_WIDTH;
            destBuf.setFloat(index, evaluate(buf.getFloat(index)));
          }
          return dest;
        }
      case FLOAT8:
        {
          Float8Vector dest =
              (Float8Vector) ValueVectors.create(context.getAllocator(), Types.MinorType.FLOAT8);
          ArrowBuf destBuf = dest.getDataBuffer();
          for (long i = 0; i < rowCount; i++) {
            long index = i * Float8Vector.TYPE_WIDTH;
            destBuf.setDouble(index, evaluate(buf.getDouble(index)));
          }
          return dest;
        }
      default:
        throw new IllegalStateException("Unsupported type: " + vector.getMinorType());
    }
  }

  @Override
  public void close() {}

  @Override
  protected boolean allowType(int index, Types.MinorType type) {
    return Schemas.isNumeric(type);
  }
}
