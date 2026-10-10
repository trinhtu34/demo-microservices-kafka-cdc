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
