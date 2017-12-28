CREATE TABLE `ot_merges` (
  `parent_ids` varchar(255) NOT NULL,
  `diff` LONGTEXT NOT NULL,
  `min_parent_id` INT(11) DEFAULT NULL,
  `max_parent_id` INT(11) DEFAULT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`parent_ids`),
  INDEX `max_parent_id` (`max_parent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
