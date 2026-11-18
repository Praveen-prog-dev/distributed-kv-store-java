package com.praveen.kvstore;

public record RebalanceReport(int totalKeys, int replicasCopied, int replicasDropped) {
}
