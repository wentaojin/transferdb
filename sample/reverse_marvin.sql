/*
 oracle schema reverse mysql database
┌────────┬────────┬────────┬──────────────────────┐
│ #      │ ORACLE │ MYSQL  │ SUGGEST              │
├────────┼────────┼────────┼──────────────────────┤
│ Schema │ marvin │ steven │ Manual Create Schema │
└────────┴────────┴────────┴──────────────────────┘
*/
CREATE DATABASE IF NOT EXISTS steven;
/*
 oracle table reverse sql 
┌───────┬───────────────┬───────────────┬─────────┐
│ #     │ ORACLE        │ MYSQL         │ SUGGEST │
├───────┼───────────────┼───────────────┼─────────┤
│ TABLE │ marvin.T_2021 │ steven.T_2021 │ Manual  │
└───────┴───────────────┴───────────────┴─────────┘
*/
CREATE TABLE `steven`.`T_2021` (
`id` decimal(11,0) NOT NULL,
`name` varchar(10),
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T_2021]
-- the above info comes from mysql table [steven.T_2021]

/*
 oracle table reverse sql 
┌───────┬───────────┬───────────┬─────────┐
│ #     │ ORACLE    │ MYSQL     │ SUGGEST │
├───────┼───────────┼───────────┼─────────┤
│ TABLE │ marvin.T6 │ steven.T6 │ Manual  │
└───────┴───────────┴───────────┴─────────┘
*/
CREATE TABLE `steven`.`T6` (
`id` decimal(11,0) NOT NULL,
`name` varchar(10) DEFAULT 'pu''',
`sex` varchar(10) DEFAULT 10 
,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T6]
-- the above info comes from mysql table [steven.T6]

/*
 oracle table reverse sql 
┌───────┬───────────┬───────────┬─────────┐
│ #     │ ORACLE    │ MYSQL     │ SUGGEST │
├───────┼───────────┼───────────┼─────────┤
│ TABLE │ marvin.T5 │ steven.T5 │ Manual  │
└───────┴───────────┴───────────┴─────────┘
*/
CREATE TABLE `steven`.`T5` (
`id` decimal(11,0) NOT NULL,
`name` varchar(10) DEFAULT '''pu''',
`sex` varchar(10) DEFAULT 10 
,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T5]
-- the above info comes from mysql table [steven.T5]

/*
 oracle table reverse sql 
┌───────┬────────────┬────────────┬─────────┐
│ #     │ ORACLE     │ MYSQL      │ SUGGEST │
├───────┼────────────┼────────────┼─────────┤
│ TABLE │ marvin.T10 │ steven.T10 │ Manual  │
└───────┴────────────┴────────────┴─────────┘
*/
CREATE TABLE `steven`.`T10` (
`id` decimal(11,0) NOT NULL,
`name` varchar(10),
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE steven.T10 ADD UNIQUE `SYS_C0010455` (NAME);

-- the above info comes from oracle table [marvin.T10]
-- the above info comes from mysql table [steven.T10]

/*
 oracle table reverse sql 
┌───────┬───────────┬───────────┬─────────┐
│ #     │ ORACLE    │ MYSQL     │ SUGGEST │
├───────┼───────────┼───────────┼─────────┤
│ TABLE │ marvin.T2 │ steven.T2 │ Manual  │
└───────┴───────────┴───────────┴─────────┘
*/
CREATE TABLE `steven`.`T2` (
`id` decimal(11,0) NOT NULL,
`age` timestamp(3),
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T2]
-- the above info comes from mysql table [steven.T2]

/*
 oracle table reverse sql 
┌───────┬────────────────────┬────────────────────┬─────────┐
│ #     │ ORACLE             │ MYSQL              │ SUGGEST │
├───────┼────────────────────┼────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_STOCK │ steven.BMSQL_STOCK │ Manual  │
└───────┴────────────────────┴────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_STOCK` (
`s_w_id` decimal(11,0) NOT NULL,
`s_i_id` decimal(11,0) NOT NULL,
`s_quantity` decimal(11,0),
`s_ytd` decimal(11,0),
`s_order_cnt` decimal(11,0),
`s_remote_cnt` decimal(11,0),
`s_data` varchar(50),
`s_dist_01` char(24),
`s_dist_02` char(24),
`s_dist_03` char(24),
`s_dist_04` char(24),
`s_dist_05` char(24),
`s_dist_06` char(24),
`s_dist_07` char(24),
`s_dist_08` char(24),
`s_dist_09` char(24),
`s_dist_10` char(24),
PRIMARY KEY (s_w_id,s_i_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_STOCK]
-- the above info comes from mysql table [steven.BMSQL_STOCK]

/*
 oracle table reverse sql 
┌───────┬───────────────────┬───────────────────┬─────────┐
│ #     │ ORACLE            │ MYSQL             │ SUGGEST │
├───────┼───────────────────┼───────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_ITEM │ steven.BMSQL_ITEM │ Manual  │
└───────┴───────────────────┴───────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_ITEM` (
`i_id` decimal(11,0) NOT NULL,
`i_name` varchar(24),
`i_price` decimal(5,2),
`i_data` varchar(50),
`i_im_id` decimal(11,0),
PRIMARY KEY (i_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_ITEM]
-- the above info comes from mysql table [steven.BMSQL_ITEM]

/*
 oracle table reverse sql 
┌───────┬─────────────────────────┬─────────────────────────┬─────────┐
│ #     │ ORACLE                  │ MYSQL                   │ SUGGEST │
├───────┼─────────────────────────┼─────────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_ORDER_LINE │ steven.BMSQL_ORDER_LINE │ Manual  │
└───────┴─────────────────────────┴─────────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_ORDER_LINE` (
`ol_w_id` decimal(11,0) NOT NULL,
`ol_d_id` decimal(11,0) NOT NULL,
`ol_o_id` decimal(11,0) NOT NULL,
`ol_number` decimal(11,0) NOT NULL,
`ol_i_id` decimal(11,0) NOT NULL,
`ol_delivery_d` timestamp(6),
`ol_amount` decimal(6,2),
`ol_supply_w_id` decimal(11,0),
`ol_quantity` decimal(11,0),
`ol_dist_info` char(24),
PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_ORDER_LINE]
-- the above info comes from mysql table [steven.BMSQL_ORDER_LINE]

/*
 oracle table reverse sql 
┌───────┬─────────────────────┬─────────────────────┬─────────┐
│ #     │ ORACLE              │ MYSQL               │ SUGGEST │
├───────┼─────────────────────┼─────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_OORDER │ steven.BMSQL_OORDER │ Manual  │
└───────┴─────────────────────┴─────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_OORDER` (
`o_w_id` decimal(11,0) NOT NULL,
`o_d_id` decimal(11,0) NOT NULL,
`o_id` decimal(11,0) NOT NULL,
`o_c_id` decimal(11,0),
`o_carrier_id` decimal(11,0),
`o_ol_cnt` decimal(11,0),
`o_dba_local` decimal(11,0),
`o_entry_d` timestamp(6),
PRIMARY KEY (o_w_id,o_d_id,o_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE UNIQUE INDEX `BMSQL_OORDER_IDX1` ON `steven`.`BMSQL_OORDER`(O_W_ID,O_D_ID,O_CARRIER_ID,O_ID);

-- the above info comes from oracle table [marvin.BMSQL_OORDER]
-- the above info comes from mysql table [steven.BMSQL_OORDER]

/*
 oracle table reverse sql 
┌───────┬────────────────────────┬────────────────────────┬─────────┐
│ #     │ ORACLE                 │ MYSQL                  │ SUGGEST │
├───────┼────────────────────────┼────────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_NEW_ORDER │ steven.BMSQL_NEW_ORDER │ Manual  │
└───────┴────────────────────────┴────────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_NEW_ORDER` (
`no_w_id` decimal(11,0) NOT NULL,
`no_d_id` decimal(11,0) NOT NULL,
`no_o_id` decimal(11,0) NOT NULL,
PRIMARY KEY (no_w_id,no_d_id,no_o_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_NEW_ORDER]
-- the above info comes from mysql table [steven.BMSQL_NEW_ORDER]

/*
 oracle table reverse sql 
┌───────┬──────────────────────┬──────────────────────┬─────────┐
│ #     │ ORACLE               │ MYSQL                │ SUGGEST │
├───────┼──────────────────────┼──────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_HISTORY │ steven.BMSQL_HISTORY │ Manual  │
└───────┴──────────────────────┴──────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_HISTORY` (
`hist_id` decimal(11,0) NOT NULL,
`h_c_id` decimal(11,0),
`h_c_d_id` decimal(11,0),
`h_c_w_id` decimal(11,0),
`h_d_id` decimal(11,0),
`h_w_id` decimal(11,0),
`h_date` timestamp(6),
`h_amount` decimal(6,2),
`h_data` varchar(24),
PRIMARY KEY (hist_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_HISTORY]
-- the above info comes from mysql table [steven.BMSQL_HISTORY]

/*
 oracle table reverse sql 
┌───────┬───────────────────────┬───────────────────────┬─────────┐
│ #     │ ORACLE                │ MYSQL                 │ SUGGEST │
├───────┼───────────────────────┼───────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_CUSTOMER │ steven.BMSQL_CUSTOMER │ Manual  │
└───────┴───────────────────────┴───────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_CUSTOMER` (
`c_w_id` decimal(11,0) NOT NULL,
`c_d_id` decimal(11,0) NOT NULL,
`c_id` decimal(11,0) NOT NULL,
`c_discount` decimal(4,4),
`c_credit` char(2),
`c_last` varchar(16),
`c_first` varchar(16),
`c_credit_lim` decimal(12,2),
`c_balance` decimal(12,2),
`c_ytd_payment` decimal(12,2),
`c_payment_cnt` decimal(11,0),
`c_delivery_cnt` decimal(11,0),
`c_street_1` varchar(20),
`c_street_2` varchar(20),
`c_city` varchar(20),
`c_state` char(2),
`c_zip` char(9),
`c_phone` char(16),
`c_since` timestamp(6),
`c_middle` char(2),
`c_data` varchar(500),
PRIMARY KEY (c_w_id,c_d_id,c_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE INDEX `bmsql_customer_idx1` ON `steven`.`BMSQL_CUSTOMER`(C_W_ID,C_D_ID,C_LAST,C_FIRST);

-- the above info comes from oracle table [marvin.BMSQL_CUSTOMER]
-- the above info comes from mysql table [steven.BMSQL_CUSTOMER]

/*
 oracle table reverse sql 
┌───────┬───────────────────────┬───────────────────────┬─────────┐
│ #     │ ORACLE                │ MYSQL                 │ SUGGEST │
├───────┼───────────────────────┼───────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_DISTRICT │ steven.BMSQL_DISTRICT │ Manual  │
└───────┴───────────────────────┴───────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_DISTRICT` (
`d_w_id` decimal(11,0) NOT NULL,
`d_id` decimal(11,0) NOT NULL,
`d_ytd` decimal(12,2),
`d_tax` decimal(4,4),
`d_next_o_id` decimal(11,0),
`d_name` varchar(10),
`d_street_1` varchar(20),
`d_street_2` varchar(20),
`d_city` varchar(20),
`d_state` char(2),
`d_zip` char(9),
PRIMARY KEY (d_w_id,d_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_DISTRICT]
-- the above info comes from mysql table [steven.BMSQL_DISTRICT]

/*
 oracle table reverse sql 
┌───────┬────────────────────────┬────────────────────────┬─────────┐
│ #     │ ORACLE                 │ MYSQL                  │ SUGGEST │
├───────┼────────────────────────┼────────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_WAREHOUSE │ steven.BMSQL_WAREHOUSE │ Manual  │
└───────┴────────────────────────┴────────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_WAREHOUSE` (
`w_id` decimal(11,0) NOT NULL,
`w_ytd` decimal(12,2),
`w_tax` decimal(4,4),
`w_name` varchar(10),
`w_street_1` varchar(20),
`w_street_2` varchar(20),
`w_city` varchar(20),
`w_state` char(2),
`w_zip` char(9),
PRIMARY KEY (w_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_WAREHOUSE]
-- the above info comes from mysql table [steven.BMSQL_WAREHOUSE]

/*
 oracle table reverse sql 
┌───────┬─────────────────────┬─────────────────────┬─────────┐
│ #     │ ORACLE              │ MYSQL               │ SUGGEST │
├───────┼─────────────────────┼─────────────────────┼─────────┤
│ TABLE │ marvin.BMSQL_CONFIG │ steven.BMSQL_CONFIG │ Manual  │
└───────┴─────────────────────┴─────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`BMSQL_CONFIG` (
`cfg_name` varchar(30) NOT NULL,
`cfg_value` varchar(50),
PRIMARY KEY (cfg_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.BMSQL_CONFIG]
-- the above info comes from mysql table [steven.BMSQL_CONFIG]

/*
 oracle table reverse sql 
┌───────┬───────────────────┬───────────────────┬─────────┐
│ #     │ ORACLE            │ MYSQL             │ SUGGEST │
├───────┼───────────────────┼───────────────────┼─────────┤
│ TABLE │ marvin.T_2021_STU │ steven.T_2021_STU │ Manual  │
└───────┴───────────────────┴───────────────────┴─────────┘
*/
CREATE TABLE `steven`.`T_2021_STU` (
`stuid` decimal(11,0) NOT NULL,
`stuname` varchar(10) NOT NULL,
`stuname1` nchar(20) NOT NULL,
`stuname2` char(10) NOT NULL,
`stuname3` varchar(1020) NOT NULL,
`stuname4` char(1),
`gender` varchar(10) NOT NULL,
`age` tinyint NOT NULL,
`joindate` datetime,
`classid` decimal(11,0) NOT NULL,
`address` nvarchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T_2021_STU]
-- the above info comes from mysql table [steven.T_2021_STU]

/*
 oracle table reverse sql 
┌───────┬──────────────────┬──────────────────┬─────────┐
│ #     │ ORACLE           │ MYSQL            │ SUGGEST │
├───────┼──────────────────┼──────────────────┼─────────┤
│ TABLE │ marvin.NEWAUTHOR │ steven.NEWAUTHOR │ Manual  │
└───────┴──────────────────┴──────────────────┴─────────┘
*/
CREATE TABLE `steven`.`NEWAUTHOR` (
`aut_id` decimal(11,0) NOT NULL,
`aut_name` varchar(50) NOT NULL,
`country` varchar(25) NOT NULL,
`home_city` varchar(25) NOT NULL,
`book_id` varchar(15) NOT NULL,
`book_id1` varchar(15) NOT NULL,
`book_id2` varchar(15) NOT NULL,
PRIMARY KEY (aut_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE steven.NEWAUTHOR ADD UNIQUE `SYS_C0010038` (HOME_CITY);

-- the above info comes from oracle table [marvin.NEWAUTHOR]
-- the above info comes from mysql table [steven.NEWAUTHOR]

/*
 oracle table reverse sql 
┌───────┬────────────────────┬────────────────────┬─────────┐
│ #     │ ORACLE             │ MYSQL              │ SUGGEST │
├───────┼────────────────────┼────────────────────┼─────────┤
│ TABLE │ marvin.UNIQUE_TEST │ steven.UNIQUE_TEST │ Manual  │
└───────┴────────────────────┴────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`UNIQUE_TEST` (
`id` decimal(11,0) NOT NULL,
`fname` varchar(20),
`lname` varchar(20),
`address` varchar(100),
`email` varchar(40),
`loc` varchar(120) NOT NULL DEFAULT 'pc' ,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE UNIQUE INDEX `IDX_ADDRESS` ON `steven`.`UNIQUE_TEST`(ADDRESS);
ALTER TABLE steven.UNIQUE_TEST ADD UNIQUE `NAME_UNIQUE` (FNAME,LNAME);

-- the above info comes from oracle table [marvin.UNIQUE_TEST]
-- the above info comes from mysql table [steven.UNIQUE_TEST]

/*
 oracle table reverse sql 
┌───────┬─────────────────────┬─────────────────────┬─────────┐
│ #     │ ORACLE              │ MYSQL               │ SUGGEST │
├───────┼─────────────────────┼─────────────────────┼─────────┤
│ TABLE │ marvin.NEWBOOK_MAST │ steven.NEWBOOK_MAST │ Manual  │
└───────┴─────────────────────┴─────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`NEWBOOK_MAST` (
`book_id` varchar(15) NOT NULL,
`book_name` varchar(50),
`isbn_no` varchar(15) NOT NULL,
`cate_id` varchar(8),
`aut_id` decimal(11,0),
`pub_id` varchar(8),
`dt_of_pub` datetime,
`pub_lang` varchar(15),
`no_page` int,
`book_price` decimal(8,2),
PRIMARY KEY (book_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.NEWBOOK_MAST]
-- the above info comes from mysql table [steven.NEWBOOK_MAST]

/*
 oracle table reverse sql 
┌───────┬───────────┬───────────┬─────────┐
│ #     │ ORACLE    │ MYSQL     │ SUGGEST │
├───────┼───────────┼───────────┼─────────┤
│ TABLE │ marvin.T4 │ steven.T4 │ Manual  │
└───────┴───────────┴───────────┴─────────┘
*/
CREATE TABLE `steven`.`T4` (
`id` decimal(11,0) NOT NULL,
`m` decimal(11,0),
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.T4]
-- the above info comes from mysql table [steven.T4]

/*
 oracle table reverse sql 
┌───────┬────────────────┬────────────────┬─────────┐
│ #     │ ORACLE         │ MYSQL          │ SUGGEST │
├───────┼────────────────┼────────────────┼─────────┤
│ TABLE │ marvin.STUDENT │ steven.STUDENT │ Manual  │
└───────┴────────────────┴────────────────┴─────────┘
*/
CREATE TABLE `steven`.`STUDENT` (
`sno` varchar(3) NOT NULL,
`sname` varchar(9) NOT NULL,
`ssex` varchar(3) NOT NULL,
`sbirthday` datetime,
`sclass` varchar(5),
PRIMARY KEY (sno)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.STUDENT]
-- the above info comes from mysql table [steven.STUDENT]

/*
 oracle table reverse sql 
┌───────┬─────────────────────────────┬─────────────────────────────┬─────────┐
│ #     │ ORACLE                      │ MYSQL                       │ SUGGEST │
├───────┼─────────────────────────────┼─────────────────────────────┼─────────┤
│ TABLE │ marvin.LIST_PARTITION_TABLE │ steven.LIST_PARTITION_TABLE │ Manual  │
└───────┴─────────────────────────────┴─────────────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`LIST_PARTITION_TABLE` (
`name` varchar(10),
`data` varchar(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.LIST_PARTITION_TABLE]
-- the above info comes from mysql table [steven.LIST_PARTITION_TABLE]

/*
 oracle table reverse sql 
┌───────┬──────────────────┬──────────────────┬─────────┐
│ #     │ ORACLE           │ MYSQL            │ SUGGEST │
├───────┼──────────────────┼──────────────────┼─────────┤
│ TABLE │ marvin.HASH_RANG │ steven.HASH_RANG │ Manual  │
└───────┴──────────────────┴──────────────────┴─────────┘
*/
CREATE TABLE `steven`.`HASH_RANG` (
`range_column_key` datetime,
`hash_column_key` decimal(11,0),
`data` varchar(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.HASH_RANG]
-- the above info comes from mysql table [steven.HASH_RANG]

/*
 oracle table reverse sql 
┌───────┬───────────────────────────┬───────────────────────────┬─────────┐
│ #     │ ORACLE                    │ MYSQL                     │ SUGGEST │
├───────┼───────────────────────────┼───────────────────────────┼─────────┤
│ TABLE │ marvin.GPRS_CELLTOPVOL_WK │ steven.GPRS_CELLTOPVOL_WK │ Manual  │
└───────┴───────────────────────────┴───────────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`GPRS_CELLTOPVOL_WK` (
`date_cd` int NOT NULL COMMENT '日期编码',
`wk_cd` tinyint NOT NULL COMMENT '周次编码',
`city_id` bigint NOT NULL COMMENT '地市编码',
`cell_en_nam` varchar(64) NOT NULL COMMENT '小区英文名',
`cell_cn_nam` varchar(64) NOT NULL COMMENT '小区中文名',
`cell_vol` decimal(11,0) COMMENT '小区流量',
`cell_vol_pct` decimal(11,0) COMMENT '小区流量占比',
`avg_rat` decimal(11,0) COMMENT '平均速率',
PRIMARY KEY (date_cd,wk_cd,city_id,cell_en_nam,cell_cn_nam)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin  COMMENT='GPRS流量小区周分析';


-- the above info comes from oracle table [marvin.GPRS_CELLTOPVOL_WK]
-- the above info comes from mysql table [steven.GPRS_CELLTOPVOL_WK]

/*
 oracle table reverse sql 
┌───────┬──────────────────────┬──────────────────────┬─────────┐
│ #     │ ORACLE               │ MYSQL                │ SUGGEST │
├───────┼──────────────────────┼──────────────────────┼─────────┤
│ TABLE │ marvin.MOBILEMESSAGE │ steven.MOBILEMESSAGE │ Manual  │
└───────┴──────────────────────┴──────────────────────┴─────────┘
*/
CREATE TABLE `steven`.`MOBILEMESSAGE` (
`acct_month` varchar(6),
`area_no` varchar(10),
`day_id` varchar(2),
`subscrbid` varchar(20),
`svcnum` varchar(30)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- the above info comes from oracle table [marvin.MOBILEMESSAGE]
-- the above info comes from mysql table [steven.MOBILEMESSAGE]

