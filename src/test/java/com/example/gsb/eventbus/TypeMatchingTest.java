package com.example.gsb.eventbus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.example.gsb.eventbus.TestEvents.DomainEvent;
import com.example.gsb.eventbus.TestEvents.OrderCancelled;
import com.example.gsb.eventbus.TestEvents.OrderCreated;
import com.example.gsb.eventbus.TestEvents.OrderEvent;
import com.example.gsb.eventbus.TestEvents.PaymentReceived;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TypeMatchingTest {

    private final DefaultEventBus bus = new DefaultEventBus(EventBusConfig.DEFAULT);
    private final List<Object> received = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        bus.close();
    }

    @Test
    void subscriberOfSuperclassReceivesSubclassEvents() {
        bus.subscribe(OrderEvent.class, received::add);

        OrderCreated created = new OrderCreated("o-1");
        OrderCancelled cancelled = new OrderCancelled("o-2");
        bus.publish(created);
        bus.publish(cancelled);

        assertThat(received).containsExactly(created, cancelled);
    }

    @Test
    void subscriberOfInterfaceReceivesImplementations() {
        bus.subscribe(DomainEvent.class, received::add);

        OrderCreated created = new OrderCreated("o-1");
        PaymentReceived payment = new PaymentReceived("p-1");
        bus.publish(created);
        bus.publish(payment);

        assertThat(received).containsExactly(created, payment);
    }

    @Test
    void subscriberOfSubclassDoesNotReceiveSuperclassOrSiblingEvents() {
        bus.subscribe(OrderCreated.class, received::add);

        bus.publish(new OrderCancelled("o-2"));

        assertThat(received).isEmpty();
    }

    @Test
    void objectSubscriberReceivesEverything() {
        bus.subscribe(Object.class, received::add);

        bus.publish("a plain string");
        bus.publish(new OrderCreated("o-1"));

        assertThat(received).hasSize(2);
    }

    @Test
    void unrelatedTypesAreNotDelivered() {
        bus.subscribe(PaymentReceived.class, received::add);

        bus.publish(new OrderCreated("o-1"));

        assertThat(received).isEmpty();
    }

    @Test
    void unsubscribeStopsDelivery() {
        Subscription subscription = bus.subscribe(OrderEvent.class, received::add);
        subscription.unsubscribe();

        bus.publish(new OrderCreated("o-1"));

        assertThat(received).isEmpty();
    }

    @Test
    void nullEventIsRejected() {
        assertThatNullPointerException().isThrownBy(() -> bus.publish(null));
    }
}
