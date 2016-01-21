CREATE TABLE `aggregation_db_log_backup` (
  `backup_revision_id` int(11) NOT NULL,
  `log` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `partition` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `file` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `file_index` INT(11) NOT NULL,
  `position` BIGINT(20) NOT NULL,
  CONSTRAINT pk_aggregation_db_log PRIMARY KEY (`backup_revision_id`, `log`, `partition`)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;