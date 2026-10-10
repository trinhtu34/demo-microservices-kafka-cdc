package com.demo.notification.consumer;

import com.demo.notification.api.FlowProgressEvent;
import com.demo.notification.stream.FlowProgressStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderCdcFlowObserver {

    private static final Logger log = LoggerFactory.getLogger(OrderCdcFlowObserver.class);

    private final JsonMapper jsonMapper;
    private final FlowProgressStream flowProgressStream;

    public OrderCdcFlowObserver(JsonMapper jsonMapper, FlowProgressStream flowProgressStream) {
        this.jsonMapper = jsonMapper;
        this.flowProgressStream = flowProgressStream;
    }

    @KafkaListener(
            topics = "${demo.kafka.order-topic}",
            groupId = "${demo.kafka.flow-observer-group}")
    public void observe(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        try {
            String operation = operation(record);
            if ("c".equals(operation) || "r".equals(operation)) {
                ObservedOrder order = order(record.value());
                if (order != null) {
                    flowProgressStream.publish(new FlowProgressEvent(
                            order.customerId(),
                            order.orderId(),
                            "ORDER_CDC_CAPTURED",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            Instant.now()));
                }
            }
        } catch (Exception exception) {
            log.warn("Could not observe order CDC record: topic={}, partition={}, offset={}, reason={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
        } finally {
            acknowledgment.acknowledge();
        }
    }

    private String operation(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader("__op");
        if (header == null || header.value() == null || header.value().length == 0) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private ObservedOrder order(byte[] value) throws Exception {
        if (value == null || value.length == 0) {
            return null;
        }
        Object parsed = jsonMapper.readValue(value, Map.class);
        if (!(parsed instanceof Map<?, ?> row)) {
            return null;
        }
        Object id = row.get("id");
        if (!(id instanceof String text) || text.isBlank() || text.length() > 36) {
            return null;
        }
        Object rawCustomerId = row.get("customer_id");
        String customerId = rawCustomerId == null ? "USER_A" : rawCustomerId.toString().strip();
        if (!"USER_A".equals(customerId) && !"USER_B".equals(customerId)) {
            return null;
        }
        return new ObservedOrder(text.strip(), customerId);
    }

    private record ObservedOrder(String orderId, String customerId) {
    }
}
