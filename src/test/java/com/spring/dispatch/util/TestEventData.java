package com.spring.dispatch.util;

import java.util.UUID;

import com.spring.dispatch.message.OrderCreated;

public class TestEventData {

    public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item) {
        return OrderCreated.builder()
                .orderId(orderId)
                .item(item)
                .build();
    }
}
