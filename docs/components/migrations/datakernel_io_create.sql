CREATE DATABASE IF NOT EXISTS `datakernel-io`;

USE `datakernel-io`;

DROP TABLE IF EXISTS sector;
DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS destination;

CREATE TABLE IF NOT EXISTS sector
(
    path  VARCHAR(50) PRIMARY KEY NOT NULL,
    title VARCHAR(100) UNIQUE     NOT NULL
);

CREATE TABLE IF NOT EXISTS destination
(
    path  VARCHAR(50) PRIMARY KEY NOT NULL,
    title VARCHAR(100) UNIQUE     NOT NULL
);

CREATE TABLE IF NOT EXISTS docs
(
    id               INT AUTO_INCREMENT PRIMARY KEY,
    path             VARCHAR(50)  NULL,
    title            VARCHAR(100) NOT NULL,
    content          TEXT         NOT NULL,
    sector_path      VARCHAR(50)  NOT NULL,
    destination_path VARCHAR(50)  NULL,
    FOREIGN KEY (sector_path) REFERENCES sector (path),
    FOREIGN KEY (destination_path) REFERENCES destination (path)
);