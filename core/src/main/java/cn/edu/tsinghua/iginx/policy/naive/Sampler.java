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
package cn.edu.tsinghua.iginx.policy.naive;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Sampler {

  private static final Sampler instance = new Sampler();

  private final Set<String> prefixSet = new HashSet<>();

  private final List<String> prefixList = new LinkedList<>();

  private final int prefixMaxSize = 100;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private final Random random = new Random();

  private Sampler() {}

  public static Sampler getInstance() {
    return instance;
  }

  public void updatePrefix(List<String> paths) {
    lock.readLock().lock();
    if (prefixMaxSize <= prefixSet.size()) {
      lock.readLock().unlock();
      return;
    }
    lock.readLock().unlock();
    lock.writeLock().lock();
    if (prefixMaxSize <= prefixSet.size()) {
      lock.writeLock().unlock();
      return;
    }

    for (String path : paths) {
      if (path != null && !path.equals("")) {
        prefixSet.add(path);
        prefixList.add(path);
      }
    }
    lock.writeLock().unlock();
  }

  public List<String> samplePrefix(int count) {
    lock.readLock().lock();
    String[] prefixArray = new String[prefixList.size()];
    prefixList.toArray(prefixArray);
    lock.readLock().unlock();
    for (int i = 0; i < prefixList.size(); i++) {
      int next = random.nextInt(prefixList.size());
      String value = prefixArray[next];
      prefixArray[next] = prefixArray[i];
      prefixArray[i] = value;
    }
    List<String> resultList = new ArrayList<>();
    for (int i = 0; i < count && i < prefixArray.length; i++) {
      resultList.add(prefixArray[i]);
    }
    return resultList;
  }
}
