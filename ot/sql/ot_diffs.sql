CREATE TABLE `ot_diffs` (
  `scope` varchar(100) DEFAULT NULL,
  `revision_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `parent_id` int(11) NOT NULL,
  `diff` text NOT NULL,
  PRIMARY KEY (`revision_id`,`parent_id`),
  UNIQUE KEY `parent_id` (`parent_id`,`revision_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
