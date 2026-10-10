SET @source_index_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = 'notification_db'
      AND table_name = 'notifications'
      AND index_name = 'uq_notifications_source'
);

SET @source_index_migration = IF(
    @source_index_exists > 0,
    'ALTER TABLE notification_db.notifications DROP INDEX uq_notifications_source',
    'SELECT 1'
);

PREPARE notification_source_index_migration FROM @source_index_migration;
EXECUTE notification_source_index_migration;
DEALLOCATE PREPARE notification_source_index_migration;

SET @customer_id_column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'notification_db'
      AND table_name = 'notifications'
      AND column_name = 'customer_id'
);

SET @customer_id_migration = IF(
    @customer_id_column_exists > 0,
    'SELECT 1',
    'ALTER TABLE notification_db.notifications ADD COLUMN customer_id VARCHAR(16) NOT NULL DEFAULT ''USER_A'' AFTER order_id'
);

PREPARE notification_customer_id_migration FROM @customer_id_migration;
EXECUTE notification_customer_id_migration;
DEALLOCATE PREPARE notification_customer_id_migration;

SET @customer_index_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = 'notification_db'
      AND table_name = 'notifications'
      AND index_name = 'idx_notifications_customer_id_id'
);

SET @customer_index_migration = IF(
    @customer_index_exists > 0,
    'SELECT 1',
    'ALTER TABLE notification_db.notifications ADD KEY idx_notifications_customer_id_id (customer_id, id)'
);

PREPARE notification_customer_index_migration FROM @customer_index_migration;
EXECUTE notification_customer_index_migration;
DEALLOCATE PREPARE notification_customer_index_migration;
