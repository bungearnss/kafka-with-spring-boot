package com.spring.dispatch.handler;

import com.spring.dispatch.exception.NotRetryableException;
import com.spring.dispatch.exception.RetryableException;
import com.spring.dispatch.message.OrderCreated;
import com.spring.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaListener(
            id = "orderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
        log.info("Received message: partition: {} - key: {} - orderId: {} - item: {}", partition, key, payload.getOrderId(), payload.getItem());
        try {
            dispatchService.process(key, payload);
        }  catch (RetryableException e) {
            log.warn("Retractable exception: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("NotRetractable exception: {}", e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
