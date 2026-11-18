package com.praveen.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvClusterTest {

    private KvCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = KvCluster.builder().nodes(5).replicationFactor(3).writeQuorum(2).readQuorum(2).build();
    }

    @Test
    void storesOnReplicationFactorNodes() {
        cluster.put("k", "v");

        int copies = 0;
        for (String id : List.of("node-1", "node-2", "node-3", "node-4", "node-5")) {
            if (cluster.node(id).keys().contains("k")) {
                copies++;
            }
        }
        assertEquals(3, copies);
        assertEquals(Optional.of("v"), cluster.get("k"));
    }

    @Test
    void laterWriteWins() {
        cluster.put("k", "v1");
        cluster.put("k", "v2");
        assertEquals(Optional.of("v2"), cluster.get("k"));
    }

    @Test
    void deleteLeavesTombstoneThatHidesValue() {
        cluster.put("k", "v");
        cluster.delete("k");
        assertEquals(Optional.empty(), cluster.get("k"));
    }

    @Test
    void toleratesOneReplicaDown() {
        cluster.put("k", "v1");
        List<String> replicas = cluster.replicasFor("k");
        cluster.node(replicas.get(0)).crash();

        cluster.put("k", "v2");
        assertEquals(Optional.of("v2"), cluster.get("k"));
    }

    @Test
    void failsWhenQuorumCannotBeReached() {
        List<String> replicas = cluster.replicasFor("k");
        cluster.node(replicas.get(0)).crash();
        cluster.node(replicas.get(1)).crash();

        assertThrows(QuorumNotReachedException.class, () -> cluster.put("k", "v"));
        assertThrows(QuorumNotReachedException.class, () -> cluster.get("k"));
    }

    @Test
    void readRepairFixesReplicaThatMissedAWrite() {
        cluster.put("k", "old");
        String lagging = cluster.replicasFor("k").get(2);
        cluster.node(lagging).crash();
        cluster.put("k", "new");
        cluster.node(lagging).recover();

        assertEquals("old", cluster.node(lagging).read("k").orElseThrow().value());
        assertEquals(Optional.of("new"), cluster.get("k"));
        assertEquals("new", cluster.node(lagging).read("k").orElseThrow().value());
        assertTrue(cluster.readRepairCount() >= 1);
    }

    @Test
    void staleReplicaCannotResurrectDeletedKey() {
        cluster.put("k", "v");
        String lagging = cluster.replicasFor("k").get(1);
        cluster.node(lagging).crash();
        cluster.delete("k");
        cluster.node(lagging).recover();

        assertEquals(Optional.empty(), cluster.get("k"));
        assertTrue(cluster.node(lagging).read("k").orElseThrow().isTombstone());
    }

    @Test
    void dataSurvivesAddingAndRemovingNodes() {
        for (int i = 0; i < 2_000; i++) {
            cluster.put("key" + i, "value" + i);
        }

        RebalanceReport added = cluster.addNode("node-6");
        assertEquals(2_000, added.totalKeys());
        assertTrue(cluster.node("node-6").keyCount() > 0);

        cluster.removeNode("node-2");
        for (int i = 0; i < 2_000; i++) {
            assertEquals(Optional.of("value" + i), cluster.get("key" + i));
            assertFalse(cluster.replicasFor("key" + i).contains("node-2"));
        }
        // every key still has exactly N copies
        int totalReplicas = cluster.keysPerNode().values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(2_000 * 3, totalReplicas);
    }

    @Test
    void rejectsQuorumLargerThanReplicationFactor() {
        assertThrows(IllegalArgumentException.class,
                () -> KvCluster.builder().replicationFactor(3).writeQuorum(4).build());
    }
}
