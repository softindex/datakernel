CREATE TABLE `aggregation_db_revision_backup` (
  `backup_revision_id` int(11) NOT NULL,
  `id` int(11) NOT NULL,
  PRIMARY KEY (`backup_revision_id`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;