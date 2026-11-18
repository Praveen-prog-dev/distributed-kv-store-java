package com.praveen.kvstore.ring;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 is not used for security here, only because it spreads keys evenly and is
 * deterministic across JVMs (String.hashCode() clusters badly for similar keys).
 */
final class Hashing {

    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    });

    private Hashing() {
    }

    static long hash(String value) {
        byte[] digest = MD5.get().digest(value.getBytes(StandardCharsets.UTF_8));
        long h = 0;
        for (int i = 0; i < 8; i++) {
            h = (h << 8) | (digest[i] & 0xFF);
        }
        return h;
    }
}
