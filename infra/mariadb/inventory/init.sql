USE inventory_db;

CREATE TABLE IF NOT EXISTS products (
    sku VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    stock INT NOT NULL CHECK (stock >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS reservations (
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(16) NOT NULL DEFAULT 'USER_A',
    sku VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(20) NOT NULL
      CHECK (status IN ('PROCESSING', 'RESERVED', 'REJECTED')),
    reason VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cdc_errors (
    topic VARCHAR(255) NOT NULL,
    partition_id INT NOT NULL,
    record_offset BIGINT NOT NULL,
    key_data LONGBLOB NULL,
    value_data LONGBLOB NULL,
    error_message VARCHAR(500) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, partition_id, record_offset)
) ENGINE=InnoDB;

INSERT INTO products (sku, name, stock) VALUES
  ('IPHONE-15', 'iPhone 15 128GB', 10),
  ('MACBOOK-M3', 'MacBook Air M3', 3),
  ('AIRPODS-PRO', 'AirPods Pro 2', 0)
ON DUPLICATE KEY UPDATE name = VALUES(name);

CREATE USER IF NOT EXISTS 'inventory_app'@'%' IDENTIFIED BY 'inventory_password';
GRANT SELECT, UPDATE ON inventory_db.products TO 'inventory_app'@'%';
GRANT SELECT, INSERT, UPDATE ON inventory_db.reservations TO 'inventory_app'@'%';
GRANT SELECT, INSERT, UPDATE ON inventory_db.cdc_errors TO 'inventory_app'@'%';

CREATE USER IF NOT EXISTS 'debezium_inventory'@'%' IDENTIFIED BY 'inventory_dbz_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium_inventory'@'%';
