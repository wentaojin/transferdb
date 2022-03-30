TransferDB 使用手册
-------
#### 使用说明
1. 表结构定义转换
    1. 常规表定义 reverse_${sourcedb}.sql 文件
    2. 不兼容性对象 compatibility_${sourcedb}.sql 文件【外键、检查约束、分区表、索引等不兼容对象】
    3. 自定义配置表字段规则映射
        1. 数据类型自定义 【column -> table -> schema -> 内置】
           - 库级别数据类型自定义
           - 表级别数据类型自定义
           - 字段级别数据类型自定义
    4. 默认值自定义【global 全局级别】
        1. 任何 schema/table 转换都需要，内置 sysdate -> now() 转换规则
    5. 内置数据类型规则映射，[内置数据类型映射规则](buildin_rule.md)
    6. 表索引定义转换
    7. 表非空约束、外键约束、检查约束、主键约束、唯一约束转换，主键、唯一、检查、外键等约束 ORACLE ENABLED 状态才会被创建，其他状态忽略创建
    8. 注意事项
       1. 分区表统一视为普通表转换，对象输出到 compatibility_${sourcedb}.sql 文件并提供 WARN 日志关键字筛选打印，若有要求，建议 reverse 手工转换
       2. 临时表统一视为普通表转换，对象输出到 compatibility_${sourcedb}.sql 文件并提供 WARN 日志关键字筛选打印
       3. 蔟表统一视为普通表转换，对象输出到 compatibility_${sourcedb}.sql 文件并提供 WARN 日志关键字筛选打印
       4. ORACLE 唯一约束基于唯一索引的字段，下游只会创建唯一索引
       5. ORACLE 字段函数默认值保持上游值，若是下游不支持的默认值，则当手工执行表创建脚本报错
       6. ORACLE FUNCTION-BASED NORMAL、BITMAP 不兼容性索引对象输出到 compatibility_${sourcedb}.sql 文件，并提供 WARN 日志关键字筛选打印
       7. 表结构以及 Schema 定义转换忽略 Oracle 字符集统一以 utf8mb4 转换，但排序规则会根据 Oracle 排序规则予以规则转换
       8. 程序 reverse 阶段若遇到报错则进程不终止，日志最后会输出警告信息，具体错误表以及对应错误详情见 {元数据库} 内表 [table_error_detail] 数据

2. 表结构对比【以 ORACLE 为基准】
   1. 表结构对比以 ORACLE 为基准对比
      1. 若上下游对比不一致，对比详情以及相关修复 SQL 语句输出 check_${sourcedb}.sql 文件
      2. 若上游字段数少，下游字段数多会自动生成删除 SQL 语句
      3. 若上游字段数多，下游字段数少会自动生成创建 SQL 语句
   2. 注意事项
      1. 表数据类型对比以 TransferDB 内置转换规则为基准，若下游表数据类型与基准不符则输出 
      2. 索引对比会忽略索引名对比，依据索引类型直接对比索引字段是否存在，解决上下游不同索引名，同个索引字段检查不一致问题
      3. ORACLE 字符数据类型 Char / Bytes ，默认 Bytes，MySQL/TiDB 是字符长度，TransferDB 只有当 Scale 数值不一致时才输出不一致
      4. 字符集检查（only 表），匹配转换 Oracle AL32UTF8 -> UTF8MB4/ ZHS16GBK -> GBK 检查，ORACLE GBK 统一视作 UTF8MB4 检查，其他暂不支持检查
      5. 排序规则检查（only 表以及字段列），ORACLE 12.2 及以上版本按字段、表维度匹配转换检查，ORACLE 12.2 以下版本按 DB 维度匹配转换检查
      6. 上游表结构存在，下游不存在，自动生成相关表结构语句输出到 reverse_${sourcedb}.sql/compatibility_${sourcedb}.sql 文件
      7. TiDB 数据库排除外键、检查约束对比，MySQL 低版本只检查外键约束，高版本外键、检查约束都对比
      8. MySQL/TiDB timestamp 类型只支持精度 6，oracle 精度最大是 9，会检查出来但是保持原样
      9. 程序 check 阶段若遇到报错则进程不终止，日志最后会输出警告信息，具体错误表以及对应错误详情见 {元数据库} 内表 [table_error_detail] 数据

3. 对象信息收集
   1. 收集现有 ORACLE 数据库内表、索引、分区表、字段长度等信息，输出类似 AWR 报告 report_${sourcedb}.html 文件，用于评估迁移至 MySQL/TiDB 成本

4. 数据同步【ORACLE 11g 及以上版本】 
   1. 数据同步需要存在主键或者唯一键
   2. 数据同步无论 FULL / ALL 模式需要注意时间格式，ORACLE date 格式复杂，同步前可先简单验证下迁移时间格式是否存在问题，transferdb timezone PICK 数据库操作系统的时区
   3. FULL 模式【全量数据导出导入】
      1. 数据同步导出导入要求表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
      2. 注意事项：
         - 断点续传期间，配置文件可能涉及迁移表变更的配置不得更改，否则会因迁移表数不一致，而自动判定无法断点续传
         - 断点续传失败，可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入或者手工清理下游元数据库记录重新导出导入
   4. ALL 模式【全量导出导入 + 增量数据同步】
      1. 增量基于 logminer 日志数据同步，存在 logminer 同等限制，且只同步 INSERT/DELETE/UPDATE DML 以及 DROP TABLE/TRUNCATE TABLE DDL，执行过 TRUNCATE TABLE/ DROP TABLE 可能需要重新增加表附加日志
      2. 基于 logminer 日志数据同步，挖掘速率取决于重做日志磁盘+归档日志磁盘【若在归档日志中】以及 PGA 内存
      3. ALL 模式同步权限以及要求详情见下【ALL 模式同步】

5. CSV 文件数据导出【ORACLE 11g 及以上版本】

6. 数据校验【ORACLE 11g 及以上版本】
   1. 数据校验以及表结构校验以上游 ORACLE 数据库为基准，上游数据存在，下游不存在则新增，下游数据存在，上游数据不存在则删除，输出文件以参数配置 fix-sql-file 命名
   2. 数据校验上游 ORACLE 数据库（主键/唯一键/唯一索引 + NUMBER 类型字段）
      1. 表必须带有主键/唯一键/唯一索引，可以是任意类型的，否则可能出现数据对比不准，如果表不存在主键或唯一键则预检查直接报错中断
      2. 表必须带有 NUMBER 类型字段，NUMBER 类型字段可以是主键、唯一键、唯一索引或者普通索引
            1. 优先主键/唯一建/唯一索引，其次普通索引
            2. 如果未配置 where 且表 pk/uk/index 不存在 number 字段则预检查直接报错中断
   3. 可选只对比数据行数 VS 对比详情产生修复文件，只对比数据行将不会输出详情修复文件
   4. 可选自定义某张表自定义 where/index-fields 参数配置
      1. 配置文件参数 where 优先级高于 index-fields，仅当两个都配置时，以 where 为准且忽略是否存在索引
   5. 可选断点续传
      1. 断点续传期间，配置文件可能涉及迁移表变更的配置不得更改，否则会因迁移表数不一致，而自动判定无法断点续传 
      2. 断点续传失败，可通过配置 enable-checkpoint = false 自动清理断点，重新数据校验对比
   6. 数据校验功能，需要将数据库时间字符串化，'2022-03-30T15:06:09+08:00' 无法对比，数据库连接参数需禁止配置 parseTime=True&loc=Local
   7. 除预检查阶段外，程序 diff 数据校验阶段若遇到报错则进程不终止，日志最后会输出警告信息，具体错误表以及对应错误详情见 {元数据库} 内表 [table_error_detail] 数据

#### 使用事项

```
1、下载 oracle client，参考官网下载地址 https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

2、上传 oracle client 至程序运行服务器，并解压到指定目录，比如：/data1/soft/client/instantclient_19_8

3、配置程序运行环境变量 LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/data1/soft/client/instantclient_19_8
echo $LD_LIBRARY_PATH

4、配置 transferdb 参数文件，config.toml 相关参数配置说明见 conf/config.toml

5、表结构转换，[输出示例](docs/reverse_${sourcedb}.sql 以及 docs/compatibility_${sourcedb}.sql)
$ ./transferdb --config config.toml --mode prepare
$ ./transferdb --config config.toml --mode reverse

元数据库[默认 db_meta]自定义转换规则，规则优先级【字段 -> 表 -> 库 -> 内置】
文件自定义规则示例：
表 [schema_rule_map] 用于库级别自定义转换规则，库级别优先级高于内置规则
表 [table_rule_map]  用于表级别自定义转换规则，表级别优先级高于库级别、高于内置规则
表 [column_rule_map] 用于字段级别自定义转换规则，字段级别优先级高于表级别、高于库级别、高于内置规则
表 [default_value_map] 用于字段默认值自定义转换规则，优先级适用于全局

6、表结构检查(独立于表结构转换，可单独运行，校验规则使用内置规则，[输出示例](docs/check_${sourcedb}.sql)
$ ./transferdb --config config.toml --mode prepare
$ ./transferdb --config config.toml --mode check

7、收集现有 Oracle 数据库内表、索引、分区表、字段长度等信息用于评估迁移成本，[输出示例](docs/report_marvin.html)
$ ./transferdb --config config.toml --mode gather

8、数据全量抽数
$ ./transferdb --config config.toml --mode full

9、数据同步（全量 + 增量）
$ ./transferdb --config config.toml --mode all

10、CSV 文件数据导出
$ ./transferdb --config config.toml --mode csv

11、数据校验
$ ./transferdb --config config.toml --mode prepare
$ ./transferdb --config config.toml --mode diff
```
#### ALL 模式同步
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