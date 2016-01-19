CREATE TABLE `aggregation_db_log_backup` (
  `backup_revision_id` int(11) NOT NULL,
  `revision_id` int(11) NOT NULL,
  `log` varchar(100) NOT NULL DEFAULT '',
  `partition` varchar(100) NOT NULL DEFAULT '',
  `file` varchar(100) NOT NULL DEFAULT '',
  `file_index` int(11) NOT NULL,
  `position` bigint(20) NOT NULL,
  PRIMARY KEY (`backup_revision_id`,`revision_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;