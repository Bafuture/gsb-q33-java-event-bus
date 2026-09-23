package com.example.gsb.eventbus;

final class TestEvents {

    interface DomainEvent {
    }

    static class OrderEvent implements DomainEvent {
        final String orderId;

        OrderEvent(String orderId) {
            this.orderId = orderId;
        }
    }

    static final class OrderCreated extends OrderEvent {
        OrderCreated(String orderId) {
            super(orderId);
        }
    }

    static final class OrderCancelled extends OrderEvent {
        OrderCancelled(String orderId) {
            super(orderId);
        }
    }

    static final class PaymentReceived implements DomainEvent {
        final String paymentId;

        PaymentReceived(String paymentId) {
            this.paymentId = paymentId;
        }
    }

    private TestEvents() {
    }
}
