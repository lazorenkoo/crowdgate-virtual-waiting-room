package com.lazorenkoo.crowdgate.gate;

public class GateTransitionException extends RuntimeException {

    public GateTransitionException(GateStatus current, GateStatus target) {
        super("Cannot change gate from " + current + " to " + target);
    }
}
