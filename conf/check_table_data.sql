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
 oracle table [marvin.MOBILEMESSAGE] is partition type [true]
 mysql table [steven.MOBILEMESSAGE] is partition type [false]
*/
-- the above info comes from oracle table [marvin.MOBILEMESSAGE]
-- the above info comes from mysql table [steven.MOBILEMESSAGE]

/*
 oracle table indexes
 mysql table indexes
*/
CREATE INDEX idx_SYS_NC00007$ ON steven.UNIQUE_TEST(SUBSTR("FNAME",1,3));
CREATE BITMAP INDEX idx_FNAME ON steven.UNIQUE_TEST(FNAME);
CREATE INDEX idx_SYS_NC00006$ ON steven.UNIQUE_TEST("EMAIL");
/*
 oracle table columns info is different from mysql
┌────────┬───────────────────────────────────────────┬──────────────────────────────────┬─────────────────────────────────────┐
│ COLUMN │ ORACLE                                    │ MYSQL                            │ SUGGEST                             │
├────────┼───────────────────────────────────────────┼──────────────────────────────────┼─────────────────────────────────────┤
│ LOC    │ VARCHAR2(120 char) NOT NULL DEFAULT 'PC'  │ VARCHAR(120) NOT NULL DEFAULT PC │ VARCHAR(120) NOT NULL DEFAULT 'PC'  │
└────────┴───────────────────────────────────────────┴──────────────────────────────────┴─────────────────────────────────────┘
*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.UNIQUE_TEST MODIFY COLUMN LOC VARCHAR(120) NOT NULL DEFAULT 'PC' ;
-- the above info comes from oracle table [marvin.UNIQUE_TEST]
-- the above info comes from mysql table [steven.UNIQUE_TEST]

/*
 oracle table type is different from mysql table type
 oracle table [marvin.GPRS_CELLTOPVOL_WK] is partition type [true]
 mysql table [steven.GPRS_CELLTOPVOL_WK] is partition type [false]
*/
-- the above info comes from oracle table [marvin.GPRS_CELLTOPVOL_WK]
-- the above info comes from mysql table [steven.GPRS_CELLTOPVOL_WK]