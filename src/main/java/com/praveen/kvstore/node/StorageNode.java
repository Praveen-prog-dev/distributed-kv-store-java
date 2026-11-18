package com.praveen.kvstore.node;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One server in the cluster. In a real system this would sit behind an RPC call; here
 * it's an in-memory map with a switch to simulate the node going down.
 */
public class StorageNode {

    private final String id;
    private final ConcurrentHashMap<String, VersionedValue> data = new ConcurrentHashMap<>();
    private volatile boolean up = true;

    public StorageNode(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    /** Keeps whichever version is newer, so replays and out-of-order writes are harmless. */
    public void write(String key, VersionedValue value) {
        ensureUp();
        data.merge(key, value, (current, incoming) -> incoming.isNewerThan(current) ? incoming : current);
    }

    public Optional<VersionedValue> read(String key) {
        ensureUp();
        return Optional.ofNullable(data.get(key));
    }

    // used during rebalancing when this node is no longer a replica for the key
    public void drop(String key) {
        data.remove(key);
    }

    public Set<String> keys() {
        return Set.copyOf(data.keySet());
    }

    public int keyCount() {
        return data.size();
    }

    public boolean isUp() {
        return up;
    }

    public void crash() {
        up = false;
    }

    public void recover() {
        up = true;
    }

    private void ensureUp() {
        if (!up) {
            throw new NodeUnavailableException(id);
        }
    }

    @Override
    public String toString() {
        return id + (up ? "" : "(down)");
    }
}
