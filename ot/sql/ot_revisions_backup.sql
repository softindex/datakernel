CREATE TABLE `ot_revisions_backup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `snapshot` LONGTEXT,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
