package com.demo.notification.consumer;

import com.demo.notification.api.NotificationResponse;
import com.demo.notification.api.FlowProgressEvent;
import com.demo.notification.application.NotificationPersistenceService;
import com.demo.notification.stream.FlowProgressStream;
import com.demo.notification.stream.NotificationStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ReservationCdcListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationCdcListener.class);

    private final ReservationCdcParser parser;
    private final NotificationPersistenceService persistenceService;
    private final NotificationStream notificationStream;
    private final FlowProgressStream flowProgressStream;

    public ReservationCdcListener(ReservationCdcParser parser,
                                  NotificationPersistenceService persistenceService,
                                  NotificationStream notificationStream,
                                  FlowProgressStream flowProgressStream) {
        this.parser = parser;
        this.persistenceService = persistenceService;
        this.notificationStream = notificationStream;
        this.flowProgressStream = flowProgressStream;
    }

    @KafkaListener(topics = "${demo.kafka.inventory-topic}")
    public void consume(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        String operation = operation(record);
        if (operation == null) {
            quarantine(record, "CDC operation header __op is missing");
        } else if ("c".equals(operation) || "u".equals(operation) || "r".equals(operation)) {
            try {
                ReservationCdcEvent event = parser.parse(record);
                if (event.isFinal()) {
                    flowProgressStream.publish(new FlowProgressEvent(
                            event.customerId(),
                            event.orderId(),
                            "RESERVATION_CDC_CAPTURED",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            Instant.now()));
                    NotificationResponse notification = persistenceService.persist(
                            event, record.topic(), record.partition(), record.offset());
                    notificationStream.publish(notification);
                }
            } catch (InvalidCdcRecordException exception) {
                quarantine(record, exception.getMessage());
            }
        } else if (!"d".equals(operation)) {
            quarantine(record, "Unsupported CDC operation: " + operation);
        }

        acknowledgment.acknowledge();
    }

    private String operation(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader("__op");
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    private void quarantine(ConsumerRecord<byte[], byte[]> record, String reason) {
        persistenceService.quarantine(record.topic(), record.partition(), record.offset(),
                record.key(), record.value(), reason);
        log.warn("Quarantined inventory CDC record: topic={}, partition={}, offset={}, reason={}",
                record.topic(), record.partition(), record.offset(), reason);
    }
}
