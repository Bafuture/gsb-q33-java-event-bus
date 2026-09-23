package com.example.gsb.eventbus;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.BooleanSupplier;

final class Await {

    static void until(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within 5s");
        }
    }

    private Await() {
    }
}
