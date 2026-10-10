package com.demo.inventory.consumer;

import java.math.BigInteger;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderCdcParser {

    private final JsonMapper jsonMapper;

    public OrderCdcParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public OrderCdcEvent parse(ConsumerRecord<byte[], byte[]> record) {
        Map<?, ?> key = readObject(record.key(), "Kafka key");
        Map<?, ?> value = readObject(record.value(), "CDC value");
        String keyId = requiredText(key.get("id"), "Kafka key id");
        String orderId = requiredText(value.get("id"), "CDC value id");
        String customerId = customerId(value.get("customer_id"));
        String sku = requiredText(value.get("sku"), "CDC value sku");
        int quantity = requiredQuantity(value.get("quantity"));

        if (!keyId.equals(orderId)) {
            throw new InvalidCdcRecordException("Kafka key id does not match CDC value id");
        }
        if (orderId.length() > 36) {
            throw new InvalidCdcRecordException("Order id exceeds 36 characters");
        }
        if (sku.length() > 50) {
            throw new InvalidCdcRecordException("SKU exceeds 50 characters");
        }

        return new OrderCdcEvent(orderId, customerId, sku, quantity);
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
