package com.demo.notification.consumer;

import java.math.BigInteger;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ReservationCdcParser {

    private final JsonMapper jsonMapper;

    public ReservationCdcParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public ReservationCdcEvent parse(ConsumerRecord<byte[], byte[]> record) {
        Map<?, ?> key = readObject(record.key(), "Kafka key");
        Map<?, ?> value = readObject(record.value(), "CDC value");
        String keyId = requiredText(key.get("order_id"), "Kafka key order_id");
        String orderId = requiredText(value.get("order_id"), "CDC value order_id");
        String customerId = customerId(value.get("customer_id"));
        String sku = requiredText(value.get("sku"), "CDC value sku");
        int quantity = requiredQuantity(value.get("quantity"));
        String status = requiredText(value.get("status"), "CDC value status");
        Object reasonValue = value.get("reason");
        String reason = reasonValue == null ? null : requiredText(reasonValue, "CDC value reason");

        if (!"PROCESSING".equals(status) && !"RESERVED".equals(status) && !"REJECTED".equals(status)) {
            throw new InvalidCdcRecordException("CDC value status is not recognized");
        }

        if (!keyId.equals(orderId)) {
            throw new InvalidCdcRecordException("Kafka key order_id does not match CDC value order_id");
        }
        if (orderId.length() > 36 || sku.length() > 50) {
            throw new InvalidCdcRecordException("CDC order id or SKU exceeds its database limit");
        }
        if (reason != null && reason.length() > 255) {
            throw new InvalidCdcRecordException("CDC value reason exceeds 255 characters");
        }

        return new ReservationCdcEvent(orderId, customerId, sku, quantity, status, reason);
    }

    private Map<?, ?> readObject(byte[] json, String field) {
        if (json == null || json.length == 0) {
            throw new InvalidCdcRecordException(field + " is empty");
        }
        try {
            Object parsed = jsonMapper.readValue(json, Map.class);
            if (parsed instanceof Map<?, ?> object) {
                return object;
            }
            throw new InvalidCdcRecordException(field + " must be a JSON object");
        } catch (InvalidCdcRecordException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new InvalidCdcRecordException(field + " is not valid JSON", exception);
        }
    }

    private String requiredText(Object value, String field) {
        if (!(value instanceof String text) || text.isBlank()) {
            throw new InvalidCdcRecordException(field + " is missing or invalid");
        }
        return text.strip();
    }

    private String customerId(Object value) {
        if (value == null) {
            return "USER_A";
        }
        String customerId = requiredText(value, "CDC value customer_id");
        if (!"USER_A".equals(customerId) && !"USER_B".equals(customerId)) {
            throw new InvalidCdcRecordException("CDC value customer_id must be USER_A or USER_B");
        }
        return customerId;
    }

    private int requiredQuantity(Object value) {
        if (value instanceof BigInteger integer) {
            if (integer.signum() <= 0 || integer.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
                throw new InvalidCdcRecordException("CDC value quantity must be a positive integer");
            }
            return integer.intValue();
        }
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)) {
            throw new InvalidCdcRecordException("CDC value quantity is missing or invalid");
        }
        long quantity = ((Number) value).longValue();
        if (quantity < 1 || quantity > Integer.MAX_VALUE) {
            throw new InvalidCdcRecordException("CDC value quantity must be a positive integer");
        }
        return (int) quantity;
    }
}
