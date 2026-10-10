package com.demo.inventory.consumer;

import com.demo.inventory.application.InventoryReservationService;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderCdcListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCdcListener.class);

    private final OrderCdcParser parser;
    private final InventoryReservationService reservationService;

    public OrderCdcListener(OrderCdcParser parser, InventoryReservationService reservationService) {
        this.parser = parser;
        this.reservationService = reservationService;
    }

    @KafkaListener(topics = "${demo.kafka.order-topic}")
    public void consume(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        String operation = operation(record);
        if (operation == null) {
            quarantine(record, "CDC operation header __op is missing");
        } else if ("c".equals(operation) || "r".equals(operation)) {
            try {
                OrderCdcEvent order = parser.parse(record);
                boolean reserved = reservationService.reserve(order);
                if (reserved) {
                    log.info("Processed order CDC record: orderId={}, topic={}, partition={}, offset={}",
                            order.orderId(), record.topic(), record.partition(), record.offset());
                } else {
                    log.info("Order CDC record already processed: topic={}, partition={}, offset={}",
                            record.topic(), record.partition(), record.offset());
                }
            } catch (InvalidCdcRecordException exception) {
                quarantine(record, exception.getMessage());
            }
        } else if (!"u".equals(operation) && !"d".equals(operation)) {
            quarantine(record, "Unsupported CDC operation: " + operation);
        }

        acknowledgment.acknowledge();
    }

    private String operation(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader("__op");
        if (header == null || header.value() == null || header.value().length == 0) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private void quarantine(ConsumerRecord<byte[], byte[]> record, String reason) {
        reservationService.quarantine(record.topic(), record.partition(), record.offset(),
                record.key(), record.value(), reason);
        log.warn("Quarantined order CDC record: topic={}, partition={}, offset={}, reason={}",
                record.topic(), record.partition(), record.offset(), reason);
    }
}
