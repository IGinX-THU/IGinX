/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.util.recycle;

import java.lang.ref.Reference;
import java.util.function.Function;

public class ReferenceRecycler<T> implements Recycler<T> {

  private final Recycler<Reference<T>> recycler;

  private final Function<T, Reference<T>> referenceFactory;

  public ReferenceRecycler(
      Recycler<Reference<T>> recycler, Function<T, Reference<T>> referenceFactory) {
    this.recycler = recycler;
    this.referenceFactory = referenceFactory;
  }

  @Override
  public T get() {
    while (true) {
      Reference<T> ref = recycler.get();
      if (ref == null) {
        return null;
      }
      T object = ref.get();
      if (object != null) {
        return object;
      }
    }
  }

  @Override
  public void recycle(T object) {
    recycler.recycle(referenceFactory.apply(object));
  }

  @Override
  public void clear() {
    recycler.clear();
  }
}
