CREATE TABLE `aggregation_db_log` (
  `revision_id` INT(11) NOT NULL,
  `log` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `partition` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `file` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL DEFAULT '',
  `file_index` INT(11) NOT NULL,
  `position` BIGINT(20) NOT NULL,
  CONSTRAINT pk_aggregation_db_log PRIMARY KEY (`revision_id`)
) ENGINE = InnoDB DEFAULT CHARSET=utf8;