package com.example.gsb.eventbus;

public enum RejectionPolicy {

    /** Throw {@link EventRejectedException} back to the publisher. */
    ABORT,

    /** Run the delivery task on the publishing thread. */
    CALLER_RUNS,

    /** Silently drop the delivery task. */
    DISCARD
}
