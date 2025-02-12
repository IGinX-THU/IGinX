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
package cn.edu.tsinghua.iginx.metadata.sync.proposal;

public interface ProposalListener {

  /**
   * when proposal created, this method will be called.
   *
   * @param key proposal key
   * @param syncProposal proposal content
   */
  void onCreate(String key, SyncProposal syncProposal);

  /**
   * when proposal updated, this method will be called.
   *
   * @param key proposal key
   * @param beforeSyncProposal proposal content before update
   * @param afterSyncProposal proposal content after update
   */
  void onUpdate(String key, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal);
}
