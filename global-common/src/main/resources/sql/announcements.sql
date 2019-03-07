CREATE TABLE IF NOT EXISTS `{announcements}` (
  `pubKey` CHAR(129) NOT NULL,
  `announcement` TEXT NOT NULL,
  PRIMARY KEY (`pubKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
