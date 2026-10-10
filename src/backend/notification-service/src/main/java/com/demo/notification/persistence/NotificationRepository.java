package com.demo.notification.persistence;

import com.demo.notification.api.NotificationResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class NotificationRepository {

    private static final RowMapper<NotificationResponse> ROW_MAPPER = NotificationRepository::mapRow;

    private final JdbcTemplate jdbcTemplate;

    public NotificationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertIfAbsent(String orderId, String sku, int quantity,
                               String status, String reason,
                               String topic, int partition, long offset) {
        jdbcTemplate.update("""
                INSERT IGNORE INTO notifications
                    (order_id, sku, quantity, status, reason, source_topic, source_partition, source_offset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, orderId, sku, quantity, status, reason, topic, partition, offset);
    }

    public void insertErrorIfAbsent(String topic, int partition, long offset,
                                    byte[] key, byte[] value, String errorMessage) {
        String message = errorMessage == null ? "Invalid inventory CDC record" : errorMessage;
        if (message.length() > 500) {
            message = message.substring(0, 500);
        }
        jdbcTemplate.update("""
                INSERT IGNORE INTO notification_errors
                    (source_topic, source_partition, source_offset, key_data, value_data, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
                """, topic, partition, offset, key, value, message);
    }

    public Optional<NotificationResponse> findByOrderId(String orderId) {
        List<NotificationResponse> rows = jdbcTemplate.query("""
                SELECT id, order_id, sku, quantity, status, reason, created_at
                FROM notifications WHERE order_id = ?
                """, ROW_MAPPER, orderId);
        return rows.stream().findFirst();
    }

    public List<NotificationResponse> findRecent(int limit) {
        return jdbcTemplate.query("""
                SELECT id, order_id, sku, quantity, status, reason, created_at
                FROM notifications ORDER BY id DESC LIMIT ?
                """, ROW_MAPPER, limit);
    }

    private static NotificationResponse mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
        return new NotificationResponse(
                resultSet.getLong("id"),
                resultSet.getString("order_id"),
                resultSet.getString("sku"),
                resultSet.getInt("quantity"),
                resultSet.getString("status"),
                resultSet.getString("reason"),
                resultSet.getTimestamp("created_at").toInstant().toString());
    }
}
