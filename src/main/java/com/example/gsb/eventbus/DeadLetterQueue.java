package com.example.gsb.eventbus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeadLetterQueue {

    /** All dead letters, ordered by the time they entered the queue. */
    List<DeadLetter> list();

    Optional<DeadLetter> find(UUID id);

    /**
     * Removes the dead letter and re-publishes its event with the original aggregate key.
     *
     * @return {@code true} if a letter with the given id existed and was re-published
     */
    boolean republish(UUID id);

    int size();
}
