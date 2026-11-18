package com.praveen.kvstore;

public class QuorumNotReachedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QuorumNotReachedException(String operation, String key, int responses, int required) {
        super(String.format("%s of '%s' got %d responses, needed %d", operation, key, responses, required));
    }
}
