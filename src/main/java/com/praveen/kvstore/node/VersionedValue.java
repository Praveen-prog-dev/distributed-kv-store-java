package com.praveen.kvstore.node;

/**
 * A stored value plus the version it was written with. A null value is a tombstone:
 * deletes have to be versioned too, otherwise a stale replica could "resurrect" the key
 * during read repair.
 */
public record VersionedValue(String value, long version) {

    public static VersionedValue tombstone(long version) {
        return new VersionedValue(null, version);
    }

    public boolean isTombstone() {
        return value == null;
    }

    public boolean isNewerThan(VersionedValue other) {
        return other == null || version > other.version;
    }
}
