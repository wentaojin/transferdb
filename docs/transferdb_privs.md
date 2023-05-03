TransferDB 权限手册
-------
### ORACLE
#### NONCDB 架构 - 非 ALL 模式
```sql
-- 连接 NONCDB
sqlplus / as sysdba

-- 创建用户
CREATE USER ggadmin IDENTIFIED BY ggadmin;

ALTER USER ggadmin quota unlimited on users;

-- 创建权限角色
CREATE ROLE transferdb_privs;
    
-- 角色授权
GRANT DBA TO transferdb_privs;
GRANT EXECUTE_CATALOG_ROLE TO  transferdb_privs;
GRANT SELECT ON V_$DATABASE TO transferdb_privs;


-- 用户角色授权
GRANT transferdb_privs TO ggadmin;
```

#### NONCDB 架构 - ALL 模式
```sql
-- 连接 NONCDB
sqlplus / as sysdba

-- 创建用户
CREATE USER ggadmin IDENTIFIED BY ggadmin;

ALTER USER ggadmin quota unlimited on users;

-- 创建权限角色
CREATE ROLE transferdb_privs;
    
-- 角色授权
GRANT DBA TO transferdb_privs;
GRANT EXECUTE_CATALOG_ROLE TO  transferdb_privs;
GRANT SELECT ON V_$DATABASE TO transferdb_privs;
GRANT SELECT ON SYSTEM.LOGMNR_COL$ TO transferdb_privs;
GRANT SELECT ON SYSTEM.LOGMNR_OBJ$ TO transferdb_privs;
GRANT SELECT ON SYSTEM.LOGMNR_USER$ TO transferdb_privs;
GRANT SELECT ON SYSTEM.LOGMNR_UID$ TO transferdb_privs;
GRANT SELECT_CATALOG_ROLE TO transferdb_privs;
GRANT EXECUTE ON DBMS_LOGMNR TO transferdb_privs;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO transferdb_privs;

-- 仅当Oracle为12c版本时，才需要添加，否则删除此行内容
GRANT LOGMINING TO transferdb_privs;

-- 用户角色授权
GRANT transferdb_privs TO ggadmin;
```
#### CDB 架构 - 非 ALL 模式
```sql
-- 连接 CDB
sqlplus / as sysdba
ALTER SESSION SET CONTAINER =CDB$ROOT;
    
-- 创建 CDB 用户
CREATE USER c##ggadmin IDENTIFIED BY ggadmin;

ALTER USER c##ggadmin quota unlimited on users;

-- 允许 CDB 用户访问所有 PDBS
ALTER USER c##ggadmin SET CONTAINER_DATA=ALL CONTAINER =CURRENT;

-- 创建权限角色
CREATE ROLE c##transferdb_privs;
    
-- 角色授权
GRANT DBA TO c##transferdb_privs CONTAINER =ALL;
GRANT EXECUTE_CATALOG_ROLE TO  c##transferdb_privs CONTAINER =ALL;
GRANT SELECT ON V_$DATABASE TO c##transferdb_privs CONTAINER =ALL;

-- CDB 用户角色授权
GRANT c##transferdb_privs TO c##ggadmin;
```
#### CDB 架构 - ALL 模式
```sql
-- 连接 CDB
sqlplus / as sysdba
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- 开启归档【必须选项】
ALTER DATABASE archivelog;
    
-- 设置归档目录
ALTER SYSTEM SET log_archive_dest_1='location=/deploy/oracle/oradata/ARCHIVE' scope=spfile sid='*';
    
-- 最小附加日志【必须选项】
ALTER DATABASE ADD supplemental LOG DATA;

/*  
    字段附加日志 
    表级别或者库级别选其一
    一般只针对同步表开启即可【必须选项】，未开启会导致同步存在问题
*/
-- 增加库级别附加日志
ALTER DATABASE ADD supplemental LOG DATA (ALL) COLUMNS;
-- 清理库级别附加日志
ALTER DATABASE DROP supplemental LOG DATA (all) COLUMNS;
    
/* 表级别附加日志 */
--切换 ${schema-name}.${table-name} 所在 container
ALTER SESSION SET CONTAINER =ORCLPDB;

-- 增加表级别附加日志
ALTER TABLE marvin.marvin4 ADD supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin7 ADD supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin8 ADD supplemental LOG DATA (all) COLUMNS;

--清理表级别附加日志
ALTER TABLE marvin.marvin4 DROP supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin7 DROP supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin8 DROP supplemental LOG DATA (all) COLUMNS;

/* 查看附加日志 */
-- 数据库级别附加日志查看
SELECT supplemental_log_data_min min,
supplemental_log_data_pk pk,
supplemental_log_data_ui ui,
supplemental_log_data_fk fk,
supplemental_log_data_all allc
FROM v$database;

-- 表级别附加日志查看
select * from dba_log_groups where upper(owner) = upper('marvin');

-- 查看不同用户的连接数
select username,count(username) from v$session where username is not null group by username;

-- 创建 CDB 用户
CREATE USER c##ggadmin IDENTIFIED BY ggadmin;

ALTER USER c##ggadmin quota unlimited on users;

-- 允许 CDB 用户访问所有 PDBS
ALTER USER c##ggadmin SET CONTAINER_DATA=ALL CONTAINER =CURRENT;

-- 创建权限角色
CREATE ROLE c##transferdb_privs;

-- 角色授权
GRANT DBA TO c##transferdb_privs CONTAINER =ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##transferdb_privs CONTAINER =ALL;
GRANT SELECT ON V_$DATABASE TO c##transferdb_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##transferdb_privs CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##transferdb_privs CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##transferdb_privs CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##transferdb_privs CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##transferdb_privs CONTAINER=ALL;

-- CDB 用户角色授权
GRANT c##transferdb_privs TO c##ggadmin;
```






