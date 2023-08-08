Drop database IF EXISTS grupal;

CREATE DATABASE IF NOT EXISTS grupal;
USE grupal;

CREATE TABLE `taxi-zone`(
LocationID INT NOT NULL,
Borough Varchar(50),
Zone Varchar(100),
service_zone Varchar(50)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE `Borough`(
 `id_Borough` INT NOT NULL AUTO_INCREMENT,
 `Borough` Varchar(50),
  Primary key(id_Borough)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE `Sonido_presencia`(
`split`VARCHAR(15),
`sensor_id` int,
`borough` VARCHAR(50),
`year` INT,
`5-1_car-horn_presence`INT,
`5-2_car-alarm_presence`INT,
`5-4_reverse-beeper_presence`INT,
`1_engine_presence`INT,
`2_machinery-impact_presence`INT,
`3_non-machinery-impact_presence`INT,
`4_powered-saw_presence`INT,
`5_alert-signal_presence`INT,
`6_music_presence`INT,
`7_human-voice_presence`INT,
`8_dog_presence`INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci
;
CREATE TABLE `Calidad_aire`(
`Name` VARCHAR(200),
`Measure` VARCHAR(200),
`Measure Info` VARCHAR(50),
`Geo Type Name` varchar(50),
`Geo Place Name`varchar(50),
`Data Value` DECIMAL(18,9),
`Stations` VARCHAR(50),
`Year` INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci
;
#drop table `Calidad_aire`;

LOAD DATA INFILE "D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\Datasets_procesados_contaminacion\\taxi-zone.csv"
INTO TABLE `taxi-zone`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA INFILE "D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\Datasets_procesados_contaminacion\\Sonido_presencia.csv"
INTO TABLE `Sonido_presencia`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA INFILE "D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\Datasets_procesados_contaminacion\\Calidad del aire.csv"
INTO TABLE `Calidad_aire`
FIELDS TERMINATED BY ','
ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

INSERT INTO `Borough`(Borough)
SELECT DISTINCT Borough FROM `taxi-zone`;
INSERT INTO `Borough`(Borough) 
VALUE("New York City")
;

ALTER TABLE Sonido_presencia ADD id_Borough INT NOT NULL DEFAULT 0 AFTER `Borough`;
ALTER TABLE `taxi-zone` ADD id_Borough INT NOT NULL DEFAULT 0 AFTER `Borough`;
ALTER TABLE `Calidad_aire` ADD id_Geo INT NOT NULL DEFAULT 0 AFTER `Geo Place Name`;

UPDATE Sonido_presencia S JOIN Borough b
ON (S.borough = b.borough)
SET S.id_Borough = b.id_Borough;

UPDATE `taxi-zone` S JOIN Borough b
ON (S.borough = b.borough)
SET S.id_Borough = b.id_Borough;

UPDATE `Calidad_aire` S JOIN Borough b
ON (S.`Geo Place Name` = b.borough)
SET S.id_Geo= b.id_Borough;

ALTER TABLE Sonido_presencia DROP Borough;
ALTER TABLE `taxi-zone` DROP Borough;
ALTER TABLE `Calidad_aire` DROP `Geo Place Name`;

SELECT *
INTO OUTFILE 'D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\MySQL\\taxi_zone.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
FROM `taxi-zone`;

SELECT *
INTO OUTFILE 'D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\MySQL\\Sonido_presencia.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
FROM `Sonido_presencia`;


SELECT *
INTO OUTFILE 'D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\MySQL\\Borough.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
FROM `Borough`;

/*
`Name` VARCHAR(200),
`Measure` VARCHAR(200),
`Measure Info` VARCHAR(50),
`Geo Type Name` varchar(50),
`id_Geo`(tipo int agregado en las alteraciones),
`Data Value` DECIMAL(18,9),
`Stations` VARCHAR(50),
`Year` INT
*/

SELECT 
    CONCAT('"', `Name`, '"') AS `Name`,
    CONCAT('"', `Measure`, '"') AS `Measure`,
    CONCAT('"', `Measure Info`, '"') AS `Measure Info`,
    CONCAT('"', `Geo Type Name`, '"') AS `Geo Type Name`,
    `id_Geo`,
    `Data Value`,
    CONCAT('"', `Stations`, '"') AS Stations,
    `Year`
INTO OUTFILE 'D:\\Programacion\\DataScience_Henry\\Proyecto_Grupal\\MySQL\\Calidad_aire.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
FROM `Calidad_aire`;

