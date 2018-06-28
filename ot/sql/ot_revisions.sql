CREATE TABLE `ot_revisions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `level` bigint NOT NULL,
  `snapshot` longtext,
  `type` enum('NEW','HEAD','INNER') NOT NULL DEFAULT 'NEW',
  `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;