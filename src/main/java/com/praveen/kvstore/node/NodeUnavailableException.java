package com.praveen.kvstore.node;

public class NodeUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NodeUnavailableException(String nodeId) {
        super("node " + nodeId + " is not reachable");
    }
}
