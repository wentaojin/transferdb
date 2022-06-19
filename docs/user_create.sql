create tablespace MARVIN_TBS
datafile '/data2/oracle/oradata/ORCLPDB/marvin00.dbf'
size 50M autoextend on next 5M maxsize unlimited;

alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin01.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin02.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin03.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin04.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin05.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin06.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin07.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin08.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin09.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TBS add datafile '/data2/oracle/oradata/ORCLPDB/marvin10.dbf' size 50M autoextend on next 5M maxsize unlimited;


create temporary tablespace MARVIN_TMP_TBL
tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp00.dbf'
size 50m autoextend on next 50m maxsize unlimited;

alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp01.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp02.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp03.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp04.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp05.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp06.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp07.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp08.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp09.dbf' size 50M autoextend on next 5M maxsize unlimited;
alter tablespace MARVIN_TMP_TBL add tempfile '/data2/oracle/oradata/ORCLPDB/marvin_tmp10.dbf' size 50M autoextend on next 5M maxsize unlimited;


CREATE USER marvin IDENTIFIED BY marvin
DEFAULT tablespace MARVIN_TBS
temporary tablespace MARVIN_TMP_TBL;

GRANT CONNECT,RESOURCE,DBA TO marvin;