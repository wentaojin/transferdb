/*
 oracle table type is different from mysql table type
┌──────────────────────┬───────────┬────────┬───────┬───────────────┐
│ TABLE                │ PARTITION │ ORACLE │ MYSQL │ SUGGEST       │
├──────────────────────┼───────────┼────────┼───────┼───────────────┤
│ LIST_PARTITION_TABLE │ PARTITION │ true   │ false │ Manual Adjust │
└──────────────────────┴───────────┴────────┴───────┴───────────────┘
*/

-- the above info comes from oracle table [marvin.LIST_PARTITION_TABLE]
-- the above info comes from mysql table [steven.LIST_PARTITION_TABLE]

/*
 oracle table columns info is different from mysql
┌────────┬─────────────────┬────────────────────┬────────────────────────┐
│ COLUMN │ ORACLE          │ MYSQL              │ SUGGEST                │
├────────┼─────────────────┼────────────────────┼────────────────────────┤
│ ID     │ NUMBER NOT NULL │ INT(11,0) NOT NULL │ DECIMAL(11,0) NOT NULL │
└────────┴─────────────────┴────────────────────┴────────────────────────┘
*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.T4 MODIFY COLUMN ID DECIMAL(11,0) NOT NULL;

-- the above info comes from oracle table [marvin.T4]
-- the above info comes from mysql table [steven.T4]

/*
 oracle table columns info is different from mysql
┌──────────┬──────────────────────────────────┬───────────────────────────┬────────────────────────────┐
│ COLUMN   │ ORACLE                           │ MYSQL                     │ SUGGEST                    │
├──────────┼──────────────────────────────────┼───────────────────────────┼────────────────────────────┤
│ ADDRESS  │ NVARCHAR2(100 char) DEFAULT NULL │ VARCHAR(100) DEFAULT NULL │ NVARCHAR(100) DEFAULT NULL │
│ STUNAME1 │ NCHAR(20 char) NOT NULL          │ CHAR(20) NOT NULL         │ NCHAR(20) NOT NULL         │
└──────────┴──────────────────────────────────┴───────────────────────────┴────────────────────────────┘
*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.T_2021_STU MODIFY COLUMN ADDRESS NVARCHAR(100) DEFAULT NULL;
ALTER TABLE steven.T_2021_STU MODIFY COLUMN STUNAME1 NCHAR(20) NOT NULL;

-- the above info comes from oracle table [marvin.T_2021_STU]
-- the above info comes from mysql table [steven.T_2021_STU]

/*
 oracle table type is different from mysql table type
┌────────────────────┬───────────┬────────┬───────┬───────────────┐
│ TABLE              │ PARTITION │ ORACLE │ MYSQL │ SUGGEST       │
├────────────────────┼───────────┼────────┼───────┼───────────────┤
│ GPRS_CELLTOPVOL_WK │ PARTITION │ true   │ false │ Manual Adjust │
└────────────────────┴───────────┴────────┴───────┴───────────────┘
*/

-- the above info comes from oracle table [marvin.GPRS_CELLTOPVOL_WK]
-- the above info comes from mysql table [steven.GPRS_CELLTOPVOL_WK]

/*
 oracle table type is different from mysql table type
┌───────────┬───────────┬────────┬───────┬───────────────┐
│ TABLE     │ PARTITION │ ORACLE │ MYSQL │ SUGGEST       │
├───────────┼───────────┼────────┼───────┼───────────────┤
│ HASH_RANG │ PARTITION │ true   │ false │ Manual Adjust │
└───────────┴───────────┴────────┴───────┴───────────────┘
*/

-- the above info comes from oracle table [marvin.HASH_RANG]
-- the above info comes from mysql table [steven.HASH_RANG]

/*
 oracle table type is different from mysql table type
┌───────────────┬───────────┬────────┬───────┬───────────────┐
│ TABLE         │ PARTITION │ ORACLE │ MYSQL │ SUGGEST       │
├───────────────┼───────────┼────────┼───────┼───────────────┤
│ MOBILEMESSAGE │ PARTITION │ true   │ false │ Manual Adjust │
└───────────────┴───────────┴────────┴───────┴───────────────┘
*/

-- the above info comes from oracle table [marvin.MOBILEMESSAGE]
-- the above info comes from mysql table [steven.MOBILEMESSAGE]

/*
 oracle and mysql table indexes
┌─────────────┬────────────────────────────┬─────────┐
│ TABLE       │ INDEXES                    │ SUGGEST │
├─────────────┼────────────────────────────┼─────────┤
│ UNIQUE_TEST │ Oracle And Mysql Different │ Run SQL │
└─────────────┴────────────────────────────┴─────────┘
*/
CREATE INDEX idx_SYS_NC00007$ ON steven.UNIQUE_TEST(SUBSTR("FNAME",1,3));
CREATE BITMAP INDEX idx_FNAME ON steven.UNIQUE_TEST(FNAME);
CREATE INDEX idx_SYS_NC00006$ ON steven.UNIQUE_TEST("EMAIL");

-- the above info comes from oracle table [marvin.UNIQUE_TEST]
-- the above info comes from mysql table [steven.UNIQUE_TEST]