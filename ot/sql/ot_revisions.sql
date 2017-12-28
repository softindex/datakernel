CREATE TABLE `ot_revisions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `snapshot` LONGTEXT,
  `type` enum('NEW','HEAD','INNER') NOT NULL DEFAULT 'NEW',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
