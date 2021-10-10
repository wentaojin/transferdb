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
-- the above info comes from oracle table [marvin.UNIQUE_TEST]
-- the above info comes from mysql table [steven.UNIQUE_TEST]

/*
 oracle table columns info is different from mysql
 column | oracle                    | mysql                | suggest
ID     | NUMBER NOT NULL           | INT(11,0) NOT NULL   | DECIMAL(11,0) NOT NULL
AGE    | TIMESTAMP(3) DEFAULT NULL | CHAR(0) DEFAULT NULL | TIMESTAMP(3) DEFAULT NULL

*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.T2 MODIFY COLUMN ID DECIMAL(11,0) NOT NULL;
ALTER TABLE steven.T2 MODIFY COLUMN AGE TIMESTAMP(3) DEFAULT NULL;
-- the above info comes from oracle table [marvin.T2]
-- the above info comes from mysql table [steven.T2]

/*
 oracle table columns info is different from mysql
 column | oracle                          | mysql                    | suggest
H_DATA | VARCHAR2(24 bytes) DEFAULT NULL | VARCHAR(24) DEFAULT NULL | VARCHAR(24) DEFAULT NULL
H_DATE | TIMESTAMP(6) DEFAULT NULL       | DATETIME(6) DEFAULT NULL | TIMESTAMP(6) DEFAULT NULL

*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.BMSQL_HISTORY MODIFY COLUMN H_DATE TIMESTAMP(6) DEFAULT NULL;
-- the above info comes from oracle table [marvin.BMSQL_HISTORY]
-- the above info comes from mysql table [steven.BMSQL_HISTORY]

/*
 oracle table columns info is different from mysql
 column     | oracle                           | mysql                     | suggest
C_STREET_1 | VARCHAR2(20 bytes) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL
C_STATE    | CHAR(2 bytes) DEFAULT NULL       | VARCHAR(2) DEFAULT NULL   | CHAR(2) DEFAULT NULL
C_PHONE    | CHAR(16 bytes) DEFAULT NULL      | VARCHAR(16) DEFAULT NULL  | CHAR(16) DEFAULT NULL
C_SINCE    | TIMESTAMP(6) DEFAULT NULL        | DATETIME(6) DEFAULT NULL  | TIMESTAMP(6) DEFAULT NULL
C_MIDDLE   | CHAR(2 bytes) DEFAULT NULL       | VARCHAR(2) DEFAULT NULL   | CHAR(2) DEFAULT NULL
C_LAST     | VARCHAR2(16 bytes) DEFAULT NULL  | VARCHAR(16) DEFAULT NULL  | VARCHAR(16) DEFAULT NULL
C_STREET_2 | VARCHAR2(20 bytes) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL
C_CITY     | VARCHAR2(20 bytes) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL  | VARCHAR(20) DEFAULT NULL
C_DATA     | VARCHAR2(500 bytes) DEFAULT NULL | VARCHAR(500) DEFAULT NULL | VARCHAR(500) DEFAULT NULL
C_CREDIT   | CHAR(2 bytes) DEFAULT NULL       | VARCHAR(2) DEFAULT NULL   | CHAR(2) DEFAULT NULL
C_FIRST    | VARCHAR2(16 bytes) DEFAULT NULL  | VARCHAR(16) DEFAULT NULL  | VARCHAR(16) DEFAULT NULL
C_ZIP      | CHAR(9 bytes) DEFAULT NULL       | VARCHAR(9) DEFAULT NULL   | CHAR(9) DEFAULT NULL

*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.BMSQL_CUSTOMER MODIFY COLUMN C_SINCE TIMESTAMP(6) DEFAULT NULL;
-- the above info comes from oracle table [marvin.BMSQL_CUSTOMER]
-- the above info comes from mysql table [steven.BMSQL_CUSTOMER]

/*
 oracle table columns info is different from mysql
 column        | oracle                      | mysql                    | suggest
OL_DIST_INFO  | CHAR(24 bytes) DEFAULT NULL | VARCHAR(24) DEFAULT NULL | CHAR(24) DEFAULT NULL
OL_DELIVERY_D | TIMESTAMP(6) DEFAULT NULL   | DATETIME(6) DEFAULT NULL | TIMESTAMP(6) DEFAULT NULL

*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.BMSQL_ORDER_LINE MODIFY COLUMN OL_DELIVERY_D TIMESTAMP(6) DEFAULT NULL;
-- the above info comes from oracle table [marvin.BMSQL_ORDER_LINE]
-- the above info comes from mysql table [steven.BMSQL_ORDER_LINE]

/*
 oracle table type is different from mysql table type
 oracle table [marvin.GPRS_CELLTOPVOL_WK] is partition type [true]
 mysql table [steven.GPRS_CELLTOPVOL_WK] is partition type [false]
*/
-- the above info comes from oracle table [marvin.GPRS_CELLTOPVOL_WK]
-- the above info comes from mysql table [steven.GPRS_CELLTOPVOL_WK]

/*
 oracle table columns info is different from mysql
 column    | oracle                    | mysql                    | suggest
O_ENTRY_D | TIMESTAMP(6) DEFAULT NULL | DATETIME(6) DEFAULT NULL | TIMESTAMP(6) DEFAULT NULL

*/
-- oracle table columns info is different from mysql, generate fixed sql
ALTER TABLE steven.BMSQL_OORDER MODIFY COLUMN O_ENTRY_D TIMESTAMP(6) DEFAULT NULL;
-- the above info comes from oracle table [marvin.BMSQL_OORDER]
-- the above info comes from mysql table [steven.BMSQL_OORDER]

