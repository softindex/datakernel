CREATE TABLE `ot_revisions` (
  `scope` varchar(100) DEFAULT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `checkpoint` text,
  `type` enum('NEW','HEAD','INNER') NOT NULL DEFAULT 'NEW',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
