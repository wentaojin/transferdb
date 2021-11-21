### Transferdb

transferdb 用于异构数据库迁移（ ORACLE 数据库 -> MySQL/TiDB 数据库），现阶段支持的功能（原 transferdb 版本被重构）：

1. 支持表结构定义转换
   1. 表定义 reverse_${sourcedb}.sql 文件，外键、检查约束、分区表、索引不兼容性对象 compatibility_${sourcedb}.sql 文件
   2. 自定义配置表字段类型规则映射【column -> table -> schema -> 内置】
      1. 库级别数据类型自定义
      2. 表级别数据类型自定义
      3. 字段级别数据类型自定义
   3. 内置数据类型规则映射，[内置数据类型映射规则](conf/buildin_reverse_rule.md)
   4. 表索引转换 
   5. 表非空约束、外键约束、检查约束、主键约束、唯一约束转换，主键、唯一、检查、外键等约束 ORACLE ENABLED 状态才会被创建，其他状态忽略创建
   6. 注意事项
      1. 考虑 Oracle 分区表特殊且 MySQL 数据库复杂分区可能不支持，分区表统一视为普通表转换，对象输出到 compatibility_${sourcedb}.sql 文件并提供 WARN 日志【partition tables】关键字筛选打印，若有要求，建议 reverse 手工转换
      2. ORACLE 唯一约束基于唯一索引的字段，下游只会创建唯一索引 
      3. ORACLE 字段函数默认值保持上游值，若是下游不支持的默认值，则当手工执行表创建脚本报错
      4. ORACLE FUNCTION-BASED NORMAL、BITMAP 不兼容性索引对象输出到 compatibility_${sourcedb}.sql 文件，并提供 WARN 日志关键字筛选打印

2. 支持表结构对比【以 ORACLE 为基准】
   1. 不一致详情以及相关修复 SQL 语句输出 check_${sourcedb}.sql文件 
   2. 注意事项 
      1. 表结构字段对比以 ORACLE 表结构为基准，若下游表结构字段多会被忽视 
      2. 索引对比会忽略索引名对比，依据索引类型直接对比索引字段是否存在，解决上下游不同索引名，同个索引字段检查不一致问题 
      3. 表数据类型对比以 TransferDB 内置转换规则为基准，若下游表数据类型与基准不符则输出 
      4. ORACLE 字符数据类型 Char / Bytes ，默认 Bytes，MySQL/TiDB 是字符长度，TransferDB 只有当 Scale 数值不一致时才输出不一致 
      5. 表级别字符集检查，采用内置字符集 utf8mb4 检查，若下游表字符集非 utf8mb4 会被检查输出 
      6. 上游表结构存在，下游不存在，自动生成相关表结构语句输出到 check_${sourcedb}.sql文件
      
3. 支持对象信息收集
   1. 收集现有 ORACLE 数据库内表、索引、分区表、字段长度等信息输出 gather_info.txt 文件，用于评估迁移至 MySQL/TiDB 成本【检查项无输出自动屏蔽显示】

4. 支持数据同步
   1. 数据同步需要存在主键或者唯一键 
   2. 数据同步无论 FULL / ALL 模式需要注意时间格式，ORACLE date 格式复杂，同步前可先简单验证下迁移时间格式是否存在问题，transferdb timezone PICK 数据库操作系统的时区 
   3. FULL 模式【全量数据导出导入】
      1. 数据同步导出导入要求表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
      2. 并发导出导入环境下，断点续传不一定百分百可行，若断点续传失败，可通过配置 enable-checkpoint 控制重新导出导入
   4. ALL 模式【全量导出导入 + 增量数据同步】
      1. 增量基于 logminer 日志数据同步，存在 logminer 同等限制，且只同步 INSERT/DELETE/UPDATE 以及 DROP TABLE/TRUNCATE TABLE DDL，执行过 TRUNCATE TABLE/ DROP TABLE 可能需要重新增加表附加日志
      2. 基于 logminer 日志数据同步，挖掘速率取决于重做日志磁盘+归档日志磁盘【若在归档日志中】以及 PGA 内存
      3. 具体 ALL 模式同步权限以及要求详情见下【ALL 模式同步】

使用事项

```
把 oracle client instantclient-basic-linux.x64-19.8.0.0.0dbru.zip 上传解压，并配置 LD_LIBRARY_PATH 环境变量
使用方法:

1、上传解压 instantclient-basic-linux.x64-19.8.0.0.0dbru.zip（该压缩包位于 client 目录） 到指定目录，比如：/data1/soft/client/instantclient_19_8

2、查看环境变量 LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data1/soft/client/instantclient_19_8
echo $LD_LIBRARY_PATH

3、transferdb 配置文件 config.toml 样例位于 conf 目录下，[详情说明](conf/config.toml)

4、表结构转换，[输出示例](conf/reverse_${sourcedb}.sql 以及 conf/compatibility_${sourcedb}.sql)
$ ./transferdb --config config.toml --mode prepare
$ ./transferdb --config config.toml --mode reverse

元数据库[默认 db_meta]自定义转换规则，规则优先级【字段 -> 表 -> 库 -> 内置】
文件自定义规则示例：
表 [schema_data_type_map] 用于库级别数据类型自定义转换规则，库级别优先级高于内置规则
表 [table_data_type_map]  用于表级别数据类型自定义转换规则，表级别优先级高于库级别、高于内置规则
表 [column_data_type_map] 用于字段级别数据类型自定义转换规则，字段级别优先级高于表级别、高于库级别、高于内置规则

5、表结构检查(独立于表结构转换，可单独运行，校验规则使用内置规则，[输出示例](conf/check_${sourcedb}.sql)
$ ./transferdb --config config.toml --mode check

6、收集现有 Oracle 数据库内表、索引、分区表、字段长度等信息用于评估迁移成本，[输出示例](conf/gather_info.txt)
$ ./transferdb --config config.toml --mode gather

7、数据全量抽数
$ ./transferdb --config config.toml --mode full

8、数据同步（全量 + 增量）
$ ./transferdb --config config.toml --mode all
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