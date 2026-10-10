USE notification_db;

CREATE TABLE IF NOT EXISTS notifications (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    sku VARCHAR(50) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RESERVED', 'REJECTED')),
    reason VARCHAR(255) NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_notifications_order_id (order_id),
    KEY idx_notifications_source (source_topic, source_partition, source_offset)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS notification_errors (
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    key_data LONGBLOB NULL,
    value_data LONGBLOB NULL,
    error_message VARCHAR(500) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_topic, source_partition, source_offset)
) ENGINE=InnoDB;

CREATE USER IF NOT EXISTS 'notification_app'@'%' IDENTIFIED BY 'notification_password';
GRANT SELECT, INSERT ON notification_db.notifications TO 'notification_app'@'%';
GRANT SELECT, INSERT ON notification_db.notification_errors TO 'notification_app'@'%';
