package com.example.gsb.eventbus;

@FunctionalInterface
public interface EventListener<T> {

    void onEvent(T event) throws Exception;
}
