package cn.edu.tsinghua.iginx.metadata.sync.proposal;

public interface VoteListener {

  void receive(String key, SyncVote vote);

  void end(String key);
}
