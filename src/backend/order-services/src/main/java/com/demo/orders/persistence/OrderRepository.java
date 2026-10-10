package com.demo.orders.persistence;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int insertIfAbsent(String id, String sku, int quantity) {
        return jdbcTemplate.update(
                "INSERT IGNORE INTO orders (id, sku, quantity) VALUES (?, ?, ?)", id, sku, quantity);
    }

    public Optional<ExistingOrder> findById(String id) {
        List<ExistingOrder> rows = jdbcTemplate.query(
                "SELECT sku, quantity FROM orders WHERE id = ?",
                (resultSet, rowNumber) -> new ExistingOrder(
                        resultSet.getString("sku"), resultSet.getInt("quantity")),
                id);
        return rows.stream().findFirst();
    }
}
