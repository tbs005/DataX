package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestShardStatusChecker {

    class ShardNode {
        StreamShard shard;
        int depth;

        ShardNode(StreamShard shard, int depth) {
            this.shard = shard;
            this.depth = depth;
        }
    }

    private StreamShard newShard(long id, String parentId, String parentSiblingId) {
        String shardId = "shard_" + id;
        StreamShard shard = new StreamShard(shardId);
        shard.setParentId(parentId);
        shard.setParentSiblingId(parentSiblingId);
        return shard;
    }

    private ShardNode pickupShard(Map<String, ShardNode> allShards, List<String> shardsToPickup, Random random) {
        int index = random.nextInt(shardsToPickup.size());
        String shardId = shardsToPickup.remove(index);
        return allShards.get(shardId);
    }

    private List<String> generateShards(int maxDepth, int maxShardCount, int rootShardCount, Map<String, ShardNode> allShards) {
        AtomicLong index = new AtomicLong(0);
        Random random = new Random(System.currentTimeMillis());
        List<String> childNodes = new LinkedList<String>();

        List<String> rootNodes = new ArrayList<String>();
        for (int i = 0; i < rootShardCount; i++) {
            StreamShard shard = newShard(index.incrementAndGet(), null, null);
            allShards.put(shard.getShardId(), new ShardNode(shard, 1));
            childNodes.add(shard.getShardId());
            rootNodes.add(shard.getShardId());
        }

        while (index.get() < maxShardCount && !childNodes.isEmpty()) {
            ShardNode shard = pickupShard(allShards, childNodes, random);
            int type = random.nextInt() % 2;
            if (type == 0 && !childNodes.isEmpty()) { // merge
                ShardNode siblingShard = pickupShard(allShards, childNodes, random);
                StreamShard c = newShard(index.incrementAndGet(), shard.shard.getShardId(), siblingShard.shard.getShardId());

                int depth = Math.max(shard.depth, siblingShard.depth) + 1;
                allShards.put(c.getShardId(), new ShardNode(c, depth));

                if (depth < maxDepth) {
                    childNodes.add(c.getShardId());
                }
            } else { // split
                StreamShard c1 = newShard(index.incrementAndGet(), shard.shard.getShardId(), null);
                StreamShard c2 = newShard(index.incrementAndGet(), shard.shard.getShardId(), null);

                int depth = shard.depth + 1;
                allShards.put(c1.getShardId(), new ShardNode(c1, depth));
                allShards.put(c2.getShardId(), new ShardNode(c2, depth));

                if (depth < maxDepth) {
                    childNodes.add(c1.getShardId());
                    childNodes.add(c2.getShardId());
                }
            }
        }
        return rootNodes;
    }

    @Test
    public void testDetermineShardState() {
        // generate shards map in complex relationship
        Map<String, ShardNode> allShardNodes = new HashMap<String, ShardNode>();
        List<String> rootShards = generateShards(5, 100, 10, allShardNodes);

        List<ShardNode> orderShards = new ArrayList<ShardNode>(allShardNodes.values());
        Comparator<ShardNode> s = new Comparator<ShardNode>() {
            @Override
            public int compare(ShardNode o1, ShardNode o2) {
                return new Integer(o1.depth).compareTo(o2.depth);
            }
        };
        Collections.sort(orderShards, s);

        // mark checkpoint
        Map<String, ShardCheckpoint> allCheckpoints = markCheckpoint(rootShards, allShardNodes);
        markCheckpoint(rootShards, allShardNodes);

        Map<String, StreamShard> allShardToProcess = new HashMap<String, StreamShard>();
        for (ShardNode node : orderShards) {
            //System.out.println("" + node.depth + ":  " + node.shard + ", ShardCheckpoint: " + allCheckpoints.get(node.shard.getShardId()));

            if (!allCheckpoints.containsKey(node.shard.getShardId())) {
                allShardToProcess.put(node.shard.getShardId(), node.shard);
            }
        }

        Map<String, StreamShard> allShards = new HashMap<String, StreamShard>();
        for (ShardNode node : allShardNodes.values()) {
            allShards.put(node.shard.getShardId(), node.shard);
        }

        ShardStatusChecker ssc = new ShardStatusChecker();
        List<StreamShard> shardToProcess = new ArrayList<StreamShard>();
        List<StreamShard> shardNoNeedToProcess = new ArrayList<StreamShard>();
        List<StreamShard> shardBlocked = new ArrayList<StreamShard>();
        ShardStatusChecker.findShardToProcess(allShardToProcess, allShards, allCheckpoints, shardToProcess, shardNoNeedToProcess, shardBlocked);

        /**
        System.out.println(orderShards.size());
        System.out.println("RootShards:");
        for (String shardId : rootShards) {
            System.out.println(shardId);
        }

        System.out.println("Shard to process --------------");
        for (StreamShard shard : shardToProcess) {
            System.out.println(shard.getShardId());
        }
        System.out.println(shardToProcess.size());

        System.out.println("Shard no need to process --------------");
        for (StreamShard shard : shardNoNeedToProcess) {
            System.out.println(shard.getShardId());
        }
        System.out.println(shardNoNeedToProcess.size());

        System.out.println("Shard blocked--------------");
        for (StreamShard shard : shardBlocked) {
            System.out.println(shard.getShardId());
        }
        System.out.println(shardBlocked.size());
        **/

        assertEquals(allShardToProcess.size(), shardBlocked.size());
        for (StreamShard shard : shardBlocked) {
            assertTrue(allShardToProcess.containsKey(shard.getShardId()));
        }
        checkState(shardToProcess, shardNoNeedToProcess, shardBlocked, allShards, allCheckpoints);
    }

    private void checkState(
            List<StreamShard> shardToProcess,
            List<StreamShard> shardNoNeedToProcess,
            List<StreamShard> shardBlocked,
            Map<String, StreamShard> allShards,
            Map<String, ShardCheckpoint> allCheckpoints) {
        Map<String, ShardStatusChecker.ProcessState> allStates = new HashMap<String, ShardStatusChecker.ProcessState>();

        while (true) {
            int count = 0;
            for (StreamShard shard : allShards.values()) {
                if (allStates.containsKey(shard.getShardId())) {
                    continue;
                }

                if (allCheckpoints.containsKey(shard.getShardId())) {
                    count++;
                    allStates.put(shard.getShardId(),
                            allCheckpoints.get(shard.getShardId()).getCheckpoint().equals(CheckpointPosition.SHARD_END) ? ShardStatusChecker.ProcessState.DONE_REACH_END : ShardStatusChecker.ProcessState.DONE_NOT_END);
                } else {
                    if (shard.getParentId() != null && !allStates.containsKey(shard.getParentId())) {
                        continue;
                    }
                    if (shard.getParentSiblingId() != null && !allStates.containsKey(shard.getParentSiblingId())) {
                        continue;
                    }

                    ShardStatusChecker.ProcessState stateOfParent = shard.getParentId() == null ? ShardStatusChecker.ProcessState.DONE_REACH_END : allStates.get(shard.getParentId());
                    ShardStatusChecker.ProcessState stateOfParentSibling = shard.getParentSiblingId() == null ? ShardStatusChecker.ProcessState.DONE_REACH_END : allStates.get(shard.getParentSiblingId());
                    //System.out.println(shard.getShardId() + ", " + shard.getParentId() + ":" + stateOfParent + ", " + shard.getParentSiblingId() + ":" + stateOfParentSibling);

                    if (stateOfParent == ShardStatusChecker.ProcessState.SKIP || stateOfParentSibling == ShardStatusChecker.ProcessState.SKIP) {
                        allStates.put(shard.getShardId(), ShardStatusChecker.ProcessState.SKIP);
                    } else if (stateOfParent == ShardStatusChecker.ProcessState.DONE_NOT_END || stateOfParentSibling == ShardStatusChecker.ProcessState.DONE_NOT_END) {
                        allStates.put(shard.getShardId(), ShardStatusChecker.ProcessState.SKIP);
                    } else if (stateOfParent == ShardStatusChecker.ProcessState.BLOCK || stateOfParentSibling == ShardStatusChecker.ProcessState.BLOCK) {
                        allStates.put(shard.getShardId(), ShardStatusChecker.ProcessState.BLOCK);
                    } else if (stateOfParent == ShardStatusChecker.ProcessState.READY || stateOfParentSibling == ShardStatusChecker.ProcessState.READY) {
                        allStates.put(shard.getShardId(), ShardStatusChecker.ProcessState.BLOCK);
                    } else {
                        allStates.put(shard.getShardId(), ShardStatusChecker.ProcessState.READY);
                    }

                    count++;
                }
            }

            if (count == 0) {
                break;
            }
        }

        assertEquals(allStates.size(), allShards.size());
        assertEquals(allStates.size(), shardToProcess.size() + shardNoNeedToProcess.size() + shardBlocked.size() + allCheckpoints.size());

        Map<String, ShardStatusChecker.ProcessState> stateActual = new HashMap<String, ShardStatusChecker.ProcessState>();
        for (StreamShard shard : shardToProcess) {
            stateActual.put(shard.getShardId(), ShardStatusChecker.ProcessState.READY);
        }
        for (StreamShard shard : shardNoNeedToProcess) {
            stateActual.put(shard.getShardId(), ShardStatusChecker.ProcessState.SKIP);
        }
        for (StreamShard shard : shardBlocked) {
            stateActual.put(shard.getShardId(), ShardStatusChecker.ProcessState.BLOCK);
        }

        assertEquals(allStates.size() - allCheckpoints.size(), stateActual.size());

        for (Map.Entry<String, ShardStatusChecker.ProcessState> entry : stateActual.entrySet()) {
            assertTrue(allStates.containsKey(entry.getKey()));
            if (allStates.get(entry.getKey()) != entry.getValue()) {
                System.out.println(entry.getKey());
            }
            assertEquals(allStates.get(entry.getKey()), entry.getValue());
        }
    }

    private Map<String, ShardCheckpoint> markCheckpoint(List<String> rootShards, Map<String, ShardNode> allShards) {
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        Set<String> marked = new HashSet<String>();
        Random random = new Random(System.currentTimeMillis());

        while (true) {
            int markedNodes = 0;
            for (Map.Entry<String, ShardNode> node : allShards.entrySet()) {
                String shardId = node.getKey();
                StreamShard shard = node.getValue().shard;

                if (marked.contains(shardId)) {
                    continue;
                }

                boolean markable = true;
                if (shard.getParentId() != null
                        && !(allCheckpoints.containsKey(shard.getParentId()) && allCheckpoints.get(shard.getParentId()).getCheckpoint().equals(CheckpointPosition.SHARD_END))) {
                    markable = false;
                }

                if (shard.getParentSiblingId() != null
                        && !(allCheckpoints.containsKey(shard.getParentSiblingId()) && allCheckpoints.get(shard.getParentSiblingId()).getCheckpoint().equals(CheckpointPosition.SHARD_END))) {
                    markable = false;
                }

                if (markable) {
                    markedNodes++;

                    marked.add(shardId);
                    switch (random.nextInt() % 3) {
                        case 0:
                            break;
                        case 1:
                            allCheckpoints.put(shardId,
                                    new ShardCheckpoint(shardId, "version", "shard_iterator", 0));
                            break;
                        case 2:
                            allCheckpoints.put(shardId,
                                    new ShardCheckpoint(shardId, "version", CheckpointPosition.SHARD_END, 0));
                            break;
                    }

                }
            }

            if (markedNodes == 0) {
                break;
            }
        }

        return allCheckpoints;
    }
}
