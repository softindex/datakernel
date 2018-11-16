CREATE TABLE IF NOT EXISTS `{backup}` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `level` bigint NOT NULL,
  `snapshot` longtext,
  `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
