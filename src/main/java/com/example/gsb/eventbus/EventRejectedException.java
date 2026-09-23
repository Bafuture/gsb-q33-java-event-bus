package com.example.gsb.eventbus;

public class EventRejectedException extends RuntimeException {

    public EventRejectedException(String message) {
        super(message);
    }
}
