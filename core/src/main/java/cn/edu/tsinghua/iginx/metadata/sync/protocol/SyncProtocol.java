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
package cn.edu.tsinghua.iginx.metadata.sync.protocol;

import cn.edu.tsinghua.iginx.metadata.sync.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncVote;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.VoteListener;

public interface SyncProtocol {

  /**
   * start a sync proposal
   *
   * @param key proposal key
   * @param syncProposal proposal content
   * @param listener vote listener
   * @return success or failure
   */
  boolean startProposal(String key, SyncProposal syncProposal, VoteListener listener)
      throws NetworkException;

  /**
   * register proposal listener, when proposal create/update/delete, listener will receive
   * notification
   *
   * @param listener proposal listener
   */
  void registerProposalListener(ProposalListener listener);

  /**
   * vote for proposal
   *
   * @param key proposal key
   * @param vote proposal vote
   */
  void voteFor(String key, SyncVote vote) throws NetworkException, VoteExpiredException;

  /**
   * end proposal
   *
   * @param key proposal key
   * @param syncProposal proposal content
   */
  void endProposal(String key, SyncProposal syncProposal)
      throws NetworkException, ExecutionException;

  void close();
}
