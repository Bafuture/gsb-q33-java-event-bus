package com.example.gsb.eventbus;

public enum DispatchMode {

    /** The listener runs on the publishing thread before {@code publish} returns. */
    SYNC,

    /** The listener runs on a worker thread of the bus-internal executor. */
    ASYNC
}
