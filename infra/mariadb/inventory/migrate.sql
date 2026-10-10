SET @customer_id_column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'inventory_db'
      AND table_name = 'reservations'
      AND column_name = 'customer_id'
);

SET @customer_id_migration = IF(
    @customer_id_column_exists > 0,
    'SELECT 1',
    'ALTER TABLE inventory_db.reservations ADD COLUMN customer_id VARCHAR(16) NOT NULL DEFAULT ''USER_A'' AFTER order_id'
);

PREPARE inventory_customer_id_migration FROM @customer_id_migration;
EXECUTE inventory_customer_id_migration;
DEALLOCATE PREPARE inventory_customer_id_migration;
