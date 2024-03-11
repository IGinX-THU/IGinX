ps -efww | grep -w 'QuorumPeerMain' | grep -v grep | cut -c 9-15 | xargs kill -9
ps -efww | grep -w 'Iginx'| grep -v grep | cut -c 9-15 | xargs kill -9

jps | grep -w 'QuorumPeerMain' | cut -c 1-6 | xargs kill -9
jps |grep -w 'Iginx'| cut -c 1-6 | xargs kill -9

