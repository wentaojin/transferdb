### Transferdb

transferdb 用于异构数据库迁移（ Oracle 数据库 -> MySQL 数据库），现阶段支持的功能（原 transferdb 版本被重构）：

1. 支持表结构定义转换
   1. 考虑 Oracle 分区表特殊且 MySQL 数据库复杂分区可能不支持，分区表统一视为普通表转换，但是 reverse 阶段日志中会打印警告【partition tables】，若有要求，建议 reverse 之后检查，需手工转换
   2. 支持自定义配置表字段类型规则转换(table -> schema -> 内置)
   3. 支持默认配置规则转换
   4. 下游若遇到同名表，则进行 rename 原表 _bak 为结尾，然后创建表
2. 支持表索引创建
   1. 排除基于函数的索引或者位图索引创建(日志输出打印，可以搜索 WARN 日志以及 FUNCTION-BASED NORMAL、BITMAP 关键筛选过滤)
   2. 下游若遇到同名索引，则进行索引名_ping 以 _ping 为后缀重创建
3. 支持非空约束、外键约束、检查约束、主键约束、唯一约束创建，生效与否取决于下游数据库
4. 数据同步【数据同步需要存在主键或者唯一键】
   1. 数据同步无论 FULL / ALL 模式需要注意时间格式，ORACLE date 格式复杂，同步前可先简单验证下迁移时间格式是否存在问题，transferdb timezone PICK 数据库操作系统的时区
   2. FULL 模式【全量数据导出导入】
      1. 数据同步导出导入要求表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
      2. 并发导出导入环境下，断点续传不一定百分百可行，若断点续传失败，可通过配置 enable-checkpoint 控制重新导出导入
   3. ALL 模式【全量导出导入 + 增量数据同步】
      1. 增量基于 logminer 日志数据同步，存在 logminer 同等限制，且只同步 INSERT/DELETE/UPDATE 以及 DROP TABLE/TRUNCATE TABLE DDL，执行过 TRUNCATE TABLE/ DROP TABLE 可能需要重新增加表附加日志
      2. 基于 logminer 日志数据同步，挖掘速率取决于重做日志磁盘+归档日志磁盘【若在归档日志中】以及 PGA 内存
      3. 具体 ALL 模式同步权限以及要求详情见下 【ALL 模式同步】

使用事项

```
把 oracle client instantclient-basic-linux.x64-19.8.0.0.0dbru.zip 上传解压，并配置 LD_LIBRARY_PATH 环境变量
使用方法:

1、上传解压 instantclient-basic-linux.x64-19.8.0.0.0dbru.zip（该压缩包位于 client 目录） 到指定目录，比如：/data1/soft/client/instantclient_19_8

2、查看环境变量 LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data1/soft/client/instantclient_19_8
echo $LD_LIBRARY_PATH

3、transferdb 配置文件 config.toml 样例位于 conf 目录下,详情请见说明以及配置
4、$ ./transferdb --config config.toml --mode prepare

元数据库 db_meta（默认）：
表 custom_schema_column_type_maps 库表字段类型转换规则
表 custom_table_column_type_maps 表级别字段类型转换规则，表级别优先级高于库级别
```

ALL 模式同步

```sql
/*同步用户*/
/*创建表空间*/
create tablespace LOGMINER_TBS
datafile '/data1/oracle/app/oracle/oradata/orcl/logminer.dbf' 
size 50M autoextend on next 5M maxsize unlimited; 

/*创建临时表空间*/
create temporary tablespace LOGMINER_TMP_TBL  
tempfile '/data1/oracle/app/oracle/oradata/orcl/logminer_tmp.dbf' 
size 50m autoextend on next 50m maxsize 20480m;  

/*创建用户*/
CREATE USER logminer IDENTIFIED BY logminer 
DEFAULT tablespace LOGMINER_TBS
temporary tablespace LOGMINER_TMP_TBL;

/*用户角色*/
create role logminer_privs;

/*角色授权*/
grant create session,
EXECUTE_CATALOG_ROLE,
select any transaction,
select any table,
select any dictionary to logminer_privs;
grant select on SYSTEM.LOGMNR_COL$ to logminer_privs;
grant select on SYSTEM.LOGMNR_OBJ$ to logminer_privs;
grant select on SYSTEM.LOGMNR_USER$ to logminer_privs;
grant select on SYSTEM.LOGMNR_UID$ to logminer_privs;
grant select on V_$DATABASE to logminer_privs;
grant select_catalog_role to logminer_privs;
grant RESOURCE,CONNECT TO logminer_privs;
grant EXECUTE ON DBMS_LOGMNR TO logminer_privs;
grant select on v_$logmnr_contents to logminer_privs;

-- 仅当Oracle为12c版本时，才需要添加，否则删除此行内容
grant LOGMINING to logminer_privs;

/*用户授权*/
grant logminer_privs to logminer;
alter user logminer quota unlimited on users;



/* 数据库开启归档以及补充日志 */
-- 开启归档【必须选项】
alter database archivelog;
-- 最小附加日志【必须选项】
ALTER DATABASE ADD supplemental LOG DATA ;

-- 表级别或者库级别选其一，一般只针对同步表开启即可【必须选项】，未开启会导致同步存在问题
--增加表级别附加日志
ALTER TABLE marvin.marvin4 ADD supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin7 ADD supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin8 ADD supplemental LOG DATA (all) COLUMNS;

--清理表级别附加日志
ALTER TABLE marvin.marvin4 DROP supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin7 DROP supplemental LOG DATA (all) COLUMNS;
ALTER TABLE marvin.marvin8 DROP supplemental LOG DATA (all) COLUMNS;

--增加或删除库级别附加日志【库级别、表级别二选一】
ALTER DATABASE ADD supplemental LOG DATA (all) COLUMNS;
ALTER DATABASE DROP supplemental LOG DATA (all) COLUMNS;

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
```

若直接在命令行中用 `nohup` 启动程序，可能会因为 SIGHUP 信号而退出，建议把 `nohup` 放到脚本里面且不建议用 kill -9，如：

```shell
#!/bin/bash
nohup ./transferdb -config config.toml --mode all > nohup.out &
```