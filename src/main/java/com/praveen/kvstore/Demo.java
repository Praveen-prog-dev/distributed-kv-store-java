package com.praveen.kvstore;

import java.util.HashMap;
import java.util.Map;

public class Demo {

    private static final int KEYS = 20_000;

    public static void main(String[] args) {
        KvCluster cluster = KvCluster.builder()
                .nodes(5)
                .virtualNodes(150)
                .replicationFactor(3)
                .writeQuorum(2)
                .readQuorum(2)
                .build();

        for (int i = 0; i < KEYS; i++) {
            cluster.put("user:" + i, "profile-" + i);
        }
        System.out.println("Replicas per node (N=3): " + cluster.keysPerNode());

        Map<String, String> ownerBefore = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            ownerBefore.put("user:" + i, cluster.ownerOf("user:" + i));
        }

        RebalanceReport report = cluster.addNode("node-6");
        long movedPrimaries = ownerBefore.entrySet().stream()
                .filter(e -> !e.getValue().equals(cluster.ownerOf(e.getKey())))
                .count();
        System.out.printf("Added node-6 -> %.1f%% of keys changed primary owner (ideal: %.1f%%), %s%n",
                100.0 * movedPrimaries / KEYS, 100.0 / 6, report);
        System.out.println("Replicas per node after: " + cluster.keysPerNode());

        String key = "user:42";
        String victim = cluster.replicasFor(key).get(0);
        cluster.node(victim).crash();
        System.out.printf("%n%s crashed. Replicas of %s: %s%n", victim, key, cluster.replicasFor(key));
        cluster.put(key, "updated-while-" + victim + "-was-down");
        System.out.println("Read with one replica down: " + cluster.get(key).orElse("<missing>"));

        cluster.node(victim).recover();
        cluster.get(key);
        System.out.println(victim + " recovered, value on it now: "
                + cluster.node(victim).read(key).orElseThrow().value()
                + " (read repairs so far: " + cluster.readRepairCount() + ")");
    }
}
