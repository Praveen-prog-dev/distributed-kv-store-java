package com.praveen.kvstore.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Consistent hash ring with virtual nodes.
 *
 * Each physical node is placed on the ring {@code virtualNodes} times. Without virtual nodes
 * a handful of servers end up owning very uneven slices of the ring; with ~100-200 per node
 * the load evens out (see ConsistentHashRingTest#virtualNodesBalanceTheLoad).
 *
 * Lookups vastly outnumber membership changes, so a read/write lock fits well here.
 */
public class ConsistentHashRing<N> {

    private final int virtualNodes;
    private final Function<N, String> nameOf;
    private final NavigableMap<Long, N> ring = new TreeMap<>();
    private final Set<N> members = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(int virtualNodes, Function<N, String> nameOf) {
        if (virtualNodes < 1) {
            throw new IllegalArgumentException("need at least one virtual node per node");
        }
        this.virtualNodes = virtualNodes;
        this.nameOf = nameOf;
    }

    public void add(N node) {
        lock.writeLock().lock();
        try {
            if (!members.add(node)) {
                return;
            }
            String name = nameOf.apply(node);
            for (int i = 0; i < virtualNodes; i++) {
                ring.put(Hashing.hash(name + "#" + i), node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(N node) {
        lock.writeLock().lock();
        try {
            if (!members.remove(node)) {
                return;
            }
            String name = nameOf.apply(node);
            for (int i = 0; i < virtualNodes; i++) {
                ring.remove(Hashing.hash(name + "#" + i), node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public N primaryFor(String key) {
        List<N> nodes = preferenceList(key, 1);
        if (nodes.isEmpty()) {
            throw new IllegalStateException("ring is empty");
        }
        return nodes.get(0);
    }

    /**
     * The first {@code count} distinct physical nodes found walking clockwise from the key's
     * position. Virtual nodes of the same server are skipped so replicas land on different machines.
     */
    public List<N> preferenceList(String key, int count) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return Collections.emptyList();
            }
            int wanted = Math.min(count, members.size());
            Set<N> result = new LinkedHashSet<>();
            long position = Hashing.hash(key);

            for (N node : ring.tailMap(position, true).values()) {
                if (result.add(node) && result.size() == wanted) {
                    return new ArrayList<>(result);
                }
            }
            for (Map.Entry<Long, N> e : ring.headMap(position, false).entrySet()) {
                if (result.add(e.getValue()) && result.size() == wanted) {
                    break;
                }
            }
            return new ArrayList<>(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return members.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
