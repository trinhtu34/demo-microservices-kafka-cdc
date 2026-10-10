SET @customer_id_column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'order_db'
      AND table_name = 'orders'
      AND column_name = 'customer_id'
);

SET @customer_id_migration = IF(
    @customer_id_column_exists > 0,
    'SELECT 1',
    'ALTER TABLE order_db.orders ADD COLUMN customer_id VARCHAR(16) NOT NULL DEFAULT ''USER_A'' AFTER id'
);

PREPARE order_customer_id_migration FROM @customer_id_migration;
EXECUTE order_customer_id_migration;
DEALLOCATE PREPARE order_customer_id_migration;
