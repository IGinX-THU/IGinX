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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.utils;

import java.util.Objects;

public class Pair<K, V> {

  public K k;

  public V v;

  public Pair(K k, V v) {
    this.k = k;
    this.v = v;
  }

  public K getK() {
    return k;
  }

  public V getV() {
    return v;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair<?, ?> pair = (Pair<?, ?>) o;
    return Objects.equals(k, pair.k) && Objects.equals(v, pair.v);
  }

  @Override
  public int hashCode() {
    return Objects.hash(k, v);
  }

  @Override
  public String toString() {
    return "Pair{" + "k=" + k + ", v=" + v + '}';
  }
}
