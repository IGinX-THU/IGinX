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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import java.util.List;
import java.util.function.Consumer;

public interface QueryClient {

  IginXTable query(final Query query) throws IginXException;

  <M> List<M> query(final Query query, final Class<M> measurementType) throws IginXException;

  IginXTable query(final String query) throws IginXException;

  void query(final Query query, final Consumer<IginXRecord> onNext) throws IginXException;

  void query(final String query, final Consumer<IginXRecord> onNext) throws IginXException;

  <M> List<M> query(final String query, final Class<M> measurementType) throws IginXException;

  <M> void query(final String query, final Class<M> measurementType, final Consumer<M> onNext)
      throws IginXException;

  <M> void query(final Query query, final Class<M> measurementType, final Consumer<M> onNext)
      throws IginXException;
}
