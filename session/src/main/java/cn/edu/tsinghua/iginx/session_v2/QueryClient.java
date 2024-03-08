/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import java.util.List;
import java.util.function.Consumer;

public interface QueryClient {

  IginXTable query(final Query query);

  <M> List<M> query(final Query query, final Class<M> measurementType);

  IginXTable query(final String query);

  void query(final Query query, final Consumer<IginXRecord> onNext);

  void query(final String query, final Consumer<IginXRecord> onNext);

  <M> List<M> query(final String query, final Class<M> measurementType);

  <M> void query(final String query, final Class<M> measurementType, final Consumer<M> onNext);

  <M> void query(final Query query, final Class<M> measurementType, final Consumer<M> onNext);
}
