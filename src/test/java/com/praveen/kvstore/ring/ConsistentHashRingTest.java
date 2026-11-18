package com.praveen.kvstore.ring;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    private static ConsistentHashRing<String> ringOf(int vnodes, String... nodes) {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(vnodes, Function.identity());
        for (String n : nodes) {
            ring.add(n);
        }
        return ring;
    }

    @Test
    void sameKeyAlwaysMapsToSameNode() {
        ConsistentHashRing<String> ring = ringOf(100, "a", "b", "c");
        String owner = ring.primaryFor("order:123");
        for (int i = 0; i < 10; i++) {
            assertEquals(owner, ring.primaryFor("order:123"));
        }
    }

    @Test
    void preferenceListHasDistinctPhysicalNodes() {
        ConsistentHashRing<String> ring = ringOf(100, "a", "b", "c", "d");
        for (int i = 0; i < 500; i++) {
            List<String> replicas = ring.preferenceList("k" + i, 3);
            assertEquals(3, replicas.size());
            assertEquals(3, new HashSet<>(replicas).size());
        }
    }

    @Test
    void preferenceListIsCappedByClusterSize() {
        ConsistentHashRing<String> ring = ringOf(50, "a", "b");
        assertEquals(2, ring.preferenceList("x", 3).size());
    }

    @Test
    void addingANodeOnlyMovesItsShareOfKeys() {
        ConsistentHashRing<String> ring = ringOf(150, "n1", "n2", "n3", "n4", "n5");
        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            before.put("key" + i, ring.primaryFor("key" + i));
        }

        ring.add("n6");

        int moved = 0;
        for (Map.Entry<String, String> e : before.entrySet()) {
            String now = ring.primaryFor(e.getKey());
            if (!now.equals(e.getValue())) {
                moved++;
                // keys only ever move TO the new node, never between old nodes
                assertEquals("n6", now);
            }
        }
        double fraction = moved / 10_000.0;
        assertTrue(fraction > 0.10 && fraction < 0.25, "moved fraction was " + fraction);
    }

    @Test
    void removingANodeOnlyAffectsItsOwnKeys() {
        ConsistentHashRing<String> ring = ringOf(150, "n1", "n2", "n3", "n4");
        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < 5_000; i++) {
            before.put("key" + i, ring.primaryFor("key" + i));
        }

        ring.remove("n2");

        before.forEach((key, oldOwner) -> {
            if (!oldOwner.equals("n2")) {
                assertEquals(oldOwner, ring.primaryFor(key));
            } else {
                assertNotEquals("n2", ring.primaryFor(key));
            }
        });
    }

    @Test
    void virtualNodesBalanceTheLoad() {
        double spreadWithout = spread(ringOf(1, "n1", "n2", "n3", "n4", "n5"));
        double spreadWith = spread(ringOf(200, "n1", "n2", "n3", "n4", "n5"));

        assertTrue(spreadWith < spreadWithout,
                "expected vnodes to even out load: " + spreadWith + " vs " + spreadWithout);
        assertTrue(spreadWith < 1.5, "max/min load ratio with vnodes was " + spreadWith);
    }

    // ratio between the busiest and the least busy node
    private static double spread(ConsistentHashRing<String> ring) {
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 20_000; i++) {
            counts.merge(ring.primaryFor("key" + i), 1, Integer::sum);
        }
        int max = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int min = counts.values().stream().mapToInt(Integer::intValue).min().orElse(1);
        return (double) max / Math.max(min, 1);
    }
}
