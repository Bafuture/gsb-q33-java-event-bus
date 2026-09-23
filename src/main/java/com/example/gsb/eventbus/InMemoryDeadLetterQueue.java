package com.example.gsb.eventbus;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public final class InMemoryDeadLetterQueue implements DeadLetterQueue {

    private final ConcurrentMap<UUID, DeadLetter> letters = new ConcurrentHashMap<>();
    private final BiConsumer<Object, Object> republisher;

    public InMemoryDeadLetterQueue(BiConsumer<Object, Object> republisher) {
        this.republisher = republisher;
    }

    void add(DeadLetter letter) {
        letters.put(letter.id(), letter);
    }

    @Override
    public List<DeadLetter> list() {
        return letters.values().stream()
                .sorted(Comparator.comparing(DeadLetter::deadAt))
                .toList();
    }

    @Override
    public Optional<DeadLetter> find(UUID id) {
        return Optional.ofNullable(letters.get(id));
    }

    @Override
    public boolean republish(UUID id) {
        DeadLetter letter = letters.remove(id);
        if (letter == null) {
            return false;
        }
        republisher.accept(letter.event(), letter.aggregateKey());
        return true;
    }

    @Override
    public int size() {
        return letters.size();
    }
}
