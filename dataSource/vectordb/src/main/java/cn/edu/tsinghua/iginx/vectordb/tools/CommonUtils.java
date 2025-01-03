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
package cn.edu.tsinghua.iginx.vectordb.tools;

import io.milvus.common.utils.Float16Utils;
import io.milvus.param.R;
import java.nio.ByteBuffer;
import java.util.*;

public class CommonUtils {

  public static void handleResponseStatus(R<?> r) {
    if (r.getStatus() != R.Status.Success.getCode()) {
      throw new RuntimeException(r.getMessage());
    }
  }

  public static List<Float> generateFloatVector(int dimension) {
    Random ran = new Random();
    List<Float> vector = new ArrayList<>();
    for (int i = 0; i < dimension; ++i) {
      vector.add(ran.nextFloat());
    }
    return vector;
  }

  public static List<Float> generateFloatVector(int dimension, Float initValue) {
    List<Float> vector = new ArrayList<>();
    for (int i = 0; i < dimension; ++i) {
      vector.add(initValue);
    }
    return vector;
  }

  public static List<List<Float>> generateFloatVectors(int dimension, int count) {
    List<List<Float>> vectors = new ArrayList<>();
    for (int n = 0; n < count; ++n) {
      List<Float> vector = generateFloatVector(dimension);
      vectors.add(vector);
    }
    return vectors;
  }

  public static List<List<Float>> generateFixFloatVectors(int dimension, int count) {
    List<List<Float>> vectors = new ArrayList<>();
    for (int n = 0; n < count; ++n) {
      List<Float> vector = generateFloatVector(dimension, (float) n);
      vectors.add(vector);
    }
    return vectors;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  public static ByteBuffer generateBinaryVector(int dimension) {
    Random ran = new Random();
    int byteCount = dimension / 8;
    // binary vector doesn't care endian since each byte is independent
    ByteBuffer vector = ByteBuffer.allocate(byteCount);
    for (int i = 0; i < byteCount; ++i) {
      vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
    }
    return vector;
  }

  public static List<ByteBuffer> generateBinaryVectors(int dimension, int count) {
    List<ByteBuffer> vectors = new ArrayList<>();
    for (int n = 0; n < count; ++n) {
      ByteBuffer vector = generateBinaryVector(dimension);
      vectors.add(vector);
    }
    return vectors;
  }

  public static ByteBuffer encodeFloat16Vector(List<Float> originVector, boolean bfloat16) {
    if (bfloat16) {
      return Float16Utils.f32VectorToBf16Buffer(originVector);
    } else {
      return Float16Utils.f32VectorToFp16Buffer(originVector);
    }
  }

  public static List<Float> decodeFloat16Vector(ByteBuffer buf, boolean bfloat16) {
    if (bfloat16) {
      return Float16Utils.bf16BufferToVector(buf);
    } else {
      return Float16Utils.fp16BufferToVector(buf);
    }
  }

  public static List<ByteBuffer> encodeFloat16Vectors(
      List<List<Float>> originVectors, boolean bfloat16) {
    List<ByteBuffer> vectors = new ArrayList<>();
    for (List<Float> originVector : originVectors) {
      if (bfloat16) {
        vectors.add(Float16Utils.f32VectorToBf16Buffer(originVector));
      } else {
        vectors.add(Float16Utils.f32VectorToFp16Buffer(originVector));
      }
    }
    return vectors;
  }

  public static ByteBuffer generateFloat16Vector(int dimension, boolean bfloat16) {
    List<Float> originalVector = generateFloatVector(dimension);
    return encodeFloat16Vector(originalVector, bfloat16);
  }

  public static List<ByteBuffer> generateFloat16Vectors(
      int dimension, int count, boolean bfloat16) {
    List<ByteBuffer> vectors = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ByteBuffer buf = generateFloat16Vector(dimension, bfloat16);
      vectors.add((buf));
    }
    return vectors;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  public static SortedMap<Long, Float> generateSparseVector() {
    Random ran = new Random();
    SortedMap<Long, Float> sparse = new TreeMap<>();
    int dim = ran.nextInt(10) + 10;
    for (int i = 0; i < dim; ++i) {
      sparse.put((long) ran.nextInt(1000000), ran.nextFloat());
    }
    return sparse;
  }

  public static List<SortedMap<Long, Float>> generateSparseVectors(int count) {
    List<SortedMap<Long, Float>> vectors = new ArrayList<>();
    for (int n = 0; n < count; ++n) {
      SortedMap<Long, Float> sparse = generateSparseVector();
      vectors.add(sparse);
    }
    return vectors;
  }
}
