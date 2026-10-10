package com.demo.inventory.application;

import com.demo.inventory.consumer.OrderCdcEvent;
import com.demo.inventory.consumer.InvalidCdcRecordException;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryReservationService {

    private final JdbcTemplate jdbcTemplate;

    public InventoryReservationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean reserve(OrderCdcEvent order) {
        int inserted = jdbcTemplate.update("""
                INSERT IGNORE INTO reservations (order_id, sku, quantity, status, reason)
                VALUES (?, ?, ?, 'PROCESSING', NULL)
                """, order.orderId(), order.sku(), order.quantity());
        if (inserted == 0) {
            List<ExistingReservation> existing = jdbcTemplate.query("""
                    SELECT sku, quantity, status
                    FROM reservations
                    WHERE order_id = ?
                    """, (resultSet, rowNumber) -> new ExistingReservation(
                    resultSet.getString("sku"),
                    resultSet.getInt("quantity"),
                    resultSet.getString("status")), order.orderId());

            ExistingReservation reservation = existing.stream().findFirst()
                    .orElseThrow(() -> new IllegalStateException("Reservation conflict row was not found"));
            if (!reservation.matches(order)) {
                throw new InvalidCdcRecordException(
                        "An order id was replayed with a different SKU or quantity");
            }
            if (!"RESERVED".equals(reservation.status()) && !"REJECTED".equals(reservation.status())) {
                throw new InvalidCdcRecordException(
                        "An existing reservation has no final status");
            }
            return false;
        }

        int stockUpdated = jdbcTemplate.update("""
                UPDATE products
                SET stock = stock - ?
                WHERE sku = ? AND stock >= ?
                """, order.quantity(), order.sku(), order.quantity());

        String status;
        String reason = null;
        if (stockUpdated == 1) {
            status = "RESERVED";
        } else {
            Integer productCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM products WHERE sku = ?", Integer.class, order.sku());
            status = "REJECTED";
            reason = productCount != null && productCount > 0
                    ? "INSUFFICIENT_STOCK"
                    : "SKU_NOT_FOUND";
        }

        jdbcTemplate.update("""
                UPDATE reservations
                SET status = ?, reason = ?
                WHERE order_id = ?
                """, status, reason, order.orderId());
        return true;
    }

    @Transactional
    public void quarantine(String topic, int partition, long offset,
                           byte[] key, byte[] value, String errorMessage) {
        String message = errorMessage == null ? "Invalid CDC record" : errorMessage;
        if (message.length() > 500) {
            message = message.substring(0, 500);
        }
        jdbcTemplate.update("""
                INSERT IGNORE INTO cdc_errors
                    (topic, partition_id, record_offset, key_data, value_data, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
                """, topic, partition, offset, key, value, message);
    }

    private record ExistingReservation(String sku, int quantity, String status) {

        private boolean matches(OrderCdcEvent order) {
            return sku.equals(order.sku()) && quantity == order.quantity();
        }
    }
}
