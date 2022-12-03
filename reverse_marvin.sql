/*
 oracle schema reverse mysql database
┌────────┬────────┬────────┬───────────────┐
│ #      │ ORACLE │ MYSQL  │ SUGGEST       │
├────────┼────────┼────────┼───────────────┤
│ Schema │ MARVIN │ MARVIN │ Create Schema │
└────────┴────────┴────────┴───────────────┘
*/
CREATE DATABASE IF NOT EXISTS MARVIN DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

/*
 oracle table reverse sql 
┌───────┬───────────────────┬────────────────┬────────────────┬──────────────┐
│ #     │ ORACLE TABLE TYPE │ ORACLE         │ MYSQL          │ SUGGEST      │
├───────┼───────────────────┼────────────────┼────────────────┼──────────────┤
│ TABLE │ HEAP              │ MARVIN.XIAMEN1 │ MARVIN.XIAMEN1 │ Create Table │
└───────┴───────────────────┴────────────────┴────────────────┴──────────────┘
*/
CREATE TABLE `MARVIN`.`XIAMEN1` (
`ID` DECIMAL(65,30) NOT NULL,
`AGE` DECIMAL(38),
PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

/*
 oracle table reverse sql 
┌───────┬───────────────────┬───────────────┬───────────────┬──────────────┐
│ #     │ ORACLE TABLE TYPE │ ORACLE        │ MYSQL         │ SUGGEST      │
├───────┼───────────────────┼───────────────┼───────────────┼──────────────┤
│ TABLE │ HEAP              │ MARVIN.XIAMEN │ MARVIN.XIAMEN │ Create Table │
└───────┴───────────────────┴───────────────┴───────────────┴──────────────┘
*/
CREATE TABLE `MARVIN`.`XIAMEN` (
`ID` DECIMAL(65,30) NOT NULL,
`DICT_KEY` VARCHAR(100) COLLATE utf8mb4_bin,
PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

/*
 oracle table reverse sql 
┌───────┬───────────────────┬────────────────┬────────────────┬──────────────┐
│ #     │ ORACLE TABLE TYPE │ ORACLE         │ MYSQL          │ SUGGEST      │
├───────┼───────────────────┼────────────────┼────────────────┼──────────────┤
│ TABLE │ HEAP              │ MARVIN.XIAMEN3 │ MARVIN.XIAMEN3 │ Create Table │
└───────┴───────────────────┴────────────────┴────────────────┴──────────────┘
*/
CREATE TABLE `MARVIN`.`XIAMEN3` (
`ID` VARCHAR(10) COLLATE utf8mb4_bin NOT NULL COMMENT 'id\&',
`NAME` VARCHAR(15) COLLATE utf8mb4_bin NOT NULL COMMENT '\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\�\"',
`AGE` DECIMAL(38) COMMENT 'age\'',
PRIMARY KEY (`ID`,`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

/*
 oracle table reverse sql 
┌───────┬───────────────────┬────────────────┬────────────────┬──────────────┐
│ #     │ ORACLE TABLE TYPE │ ORACLE         │ MYSQL          │ SUGGEST      │
├───────┼───────────────────┼────────────────┼────────────────┼──────────────┤
│ TABLE │ HEAP              │ MARVIN.XIAMEN2 │ MARVIN.XIAMEN2 │ Create Table │
└───────┴───────────────────┴────────────────┴────────────────┴──────────────┘
*/
CREATE TABLE `MARVIN`.`XIAMEN2` (
`ID` VARCHAR(10) COLLATE utf8mb4_bin NOT NULL,
`NAME` VARCHAR(15) COLLATE utf8mb4_bin NOT NULL,
`AGE` DECIMAL(38),
PRIMARY KEY (`ID`,`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

/*
 oracle table reverse sql 
┌───────┬───────────────────┬────────────────┬────────────────┬──────────────┐
│ #     │ ORACLE TABLE TYPE │ ORACLE         │ MYSQL          │ SUGGEST      │
├───────┼───────────────────┼────────────────┼────────────────┼──────────────┤
│ TABLE │ HEAP              │ MARVIN.XIAMEN4 │ MARVIN.XIAMEN4 │ Create Table │
└───────┴───────────────────┴────────────────┴────────────────┴──────────────┘
*/
CREATE TABLE `MARVIN`.`XIAMEN4` (
`ID` VARCHAR(10) COLLATE utf8mb4_bin,
`NAME` VARCHAR(2000) COLLATE utf8mb4_bin DEFAULT UUID(),
`AGE` DECIMAL(38)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4;

