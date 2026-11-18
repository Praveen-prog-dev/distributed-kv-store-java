package com.praveen.kvstore;

import com.praveen.kvstore.node.NodeUnavailableException;
import com.praveen.kvstore.node.StorageNode;
import com.praveen.kvstore.node.VersionedValue;
import com.praveen.kvstore.ring.ConsistentHashRing;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator for a Dynamo-style cluster.
 *
 * Every key is stored on N replicas (its preference list on the ring). A write succeeds
 * once W replicas acknowledge it, a read succeeds once R replicas answer. With R + W > N
 * every read quorum overlaps every write quorum, so a successful read always sees the
 * latest successful write.
 *
 * Conflicts are resolved last-write-wins using a version from a cluster-wide counter.
 * That's a simplification - see the README for why real systems use vector clocks.
 */
public class KvCluster {

    private final ConsistentHashRing<StorageNode> ring;
    private final Map<String, StorageNode> nodes = new ConcurrentHashMap<>();
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;
    private final AtomicLong versionClock = new AtomicLong();
    private final AtomicLong readRepairs = new AtomicLong();

    private KvCluster(Builder b) {
        if (b.writeQuorum > b.replicationFactor || b.readQuorum > b.replicationFactor) {
            throw new IllegalArgumentException("quorum can't be larger than the replication factor");
        }
        this.replicationFactor = b.replicationFactor;
        this.writeQuorum = b.writeQuorum;
        this.readQuorum = b.readQuorum;
        this.ring = new ConsistentHashRing<>(b.virtualNodes, StorageNode::id);
        for (int i = 1; i <= b.nodeCount; i++) {
            StorageNode node = new StorageNode("node-" + i);
            nodes.put(node.id(), node);
            ring.add(node);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void put(String key, String value) {
        if (value == null) {
            throw new IllegalArgumentException("use delete() to remove a key");
        }
        replicate(key, new VersionedValue(value, versionClock.incrementAndGet()));
    }

    public void delete(String key) {
        replicate(key, VersionedValue.tombstone(versionClock.incrementAndGet()));
    }

    public Optional<String> get(String key) {
        List<StorageNode> replicas = ring.preferenceList(key, replicationFactor);
        Map<StorageNode, Optional<VersionedValue>> answers = new LinkedHashMap<>();
        for (StorageNode replica : replicas) {
            try {
                answers.put(replica, replica.read(key));
            } catch (NodeUnavailableException ignored) {
                // a missing answer just doesn't count towards the quorum
            }
        }
        if (answers.size() < readQuorum) {
            throw new QuorumNotReachedException("read", key, answers.size(), readQuorum);
        }

        Optional<VersionedValue> latest = answers.values().stream()
                .flatMap(Optional::stream)
                .max(Comparator.comparingLong(VersionedValue::version));

        latest.ifPresent(winner -> repairStaleReplicas(key, winner, answers));
        return latest.filter(v -> !v.isTombstone()).map(VersionedValue::value);
    }

    public synchronized RebalanceReport addNode(String nodeId) {
        if (nodes.containsKey(nodeId)) {
            throw new IllegalArgumentException(nodeId + " already in cluster");
        }
        StorageNode node = new StorageNode(nodeId);
        nodes.put(nodeId, node);
        ring.add(node);
        return rebalance();
    }

    /** Planned decommission: the node leaves the ring and its keys are re-replicated elsewhere. */
    public synchronized RebalanceReport removeNode(String nodeId) {
        StorageNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("unknown node " + nodeId);
        }
        ring.remove(node);
        RebalanceReport report = rebalance();
        nodes.remove(nodeId);
        return report;
    }

    public StorageNode node(String nodeId) {
        StorageNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("unknown node " + nodeId);
        }
        return node;
    }

    public List<String> replicasFor(String key) {
        return ring.preferenceList(key, replicationFactor).stream().map(StorageNode::id).toList();
    }

    public String ownerOf(String key) {
        return ring.primaryFor(key).id();
    }

    public Map<String, Integer> keysPerNode() {
        Map<String, Integer> counts = new TreeMap<>();
        nodes.values().forEach(n -> counts.put(n.id(), n.keyCount()));
        return counts;
    }

    public long readRepairCount() {
        return readRepairs.get();
    }

    private void replicate(String key, VersionedValue value) {
        int acks = 0;
        for (StorageNode replica : ring.preferenceList(key, replicationFactor)) {
            try {
                replica.write(key, value);
                acks++;
            } catch (NodeUnavailableException ignored) {
                // Dynamo would use hinted handoff here; we rely on read repair + rebalance instead
            }
        }
        if (acks < writeQuorum) {
            throw new QuorumNotReachedException("write", key, acks, writeQuorum);
        }
    }

    private void repairStaleReplicas(String key, VersionedValue latest,
                                     Map<StorageNode, Optional<VersionedValue>> answers) {
        answers.forEach((replica, answer) -> {
            boolean stale = answer.isEmpty() || latest.isNewerThan(answer.get());
            if (stale) {
                try {
                    replica.write(key, latest);
                    readRepairs.incrementAndGet();
                } catch (NodeUnavailableException ignored) {
                    // went down between the read and the repair, next read will try again
                }
            }
        });
    }

    /**
     * Brings every key's replicas in line with the current ring. Only keys whose preference
     * list changed are touched - with consistent hashing that's roughly 1/N of them.
     */
    private RebalanceReport rebalance() {
        Map<String, VersionedValue> latestByKey = new HashMap<>();
        for (StorageNode node : nodes.values()) {
            if (!node.isUp()) {
                continue;
            }
            for (String key : node.keys()) {
                node.read(key).ifPresent(v -> latestByKey.merge(key, v, (a, b) -> b.isNewerThan(a) ? b : a));
            }
        }

        int copied = 0;
        int dropped = 0;
        for (Map.Entry<String, VersionedValue> entry : latestByKey.entrySet()) {
            String key = entry.getKey();
            VersionedValue latest = entry.getValue();
            Set<StorageNode> owners = new HashSet<>(ring.preferenceList(key, replicationFactor));

            for (StorageNode owner : owners) {
                if (owner.isUp() && owner.read(key).map(latest::isNewerThan).orElse(true)) {
                    owner.write(key, latest);
                    copied++;
                }
            }
            for (StorageNode node : nodes.values()) {
                if (!owners.contains(node) && node.isUp() && node.keys().contains(key)) {
                    node.drop(key);
                    dropped++;
                }
            }
        }
        return new RebalanceReport(latestByKey.size(), copied, dropped);
    }

    public static final class Builder {
        private int nodeCount = 5;
        private int virtualNodes = 150;
        private int replicationFactor = 3;
        private int writeQuorum = 2;
        private int readQuorum = 2;

        public Builder nodes(int count) {
            this.nodeCount = count;
            return this;
        }

        public Builder virtualNodes(int count) {
            this.virtualNodes = count;
            return this;
        }

        public Builder replicationFactor(int n) {
            this.replicationFactor = n;
            return this;
        }

        public Builder writeQuorum(int w) {
            this.writeQuorum = w;
            return this;
        }

        public Builder readQuorum(int r) {
            this.readQuorum = r;
            return this;
        }

        public KvCluster build() {
            return new KvCluster(this);
        }
    }
}
