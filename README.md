### Transferdb

transferdb 用于异构数据库迁移（ Oracle 数据库 -> MySQL 数据库），现阶段支持的功能（原 transferdb 版本被重构，新增自定义转换规则）：

1. 支持表结构定义转换
   1. 考虑 Oracle 分区表特殊且 MySQL 数据库复杂分区可能不支持，分区表统一视为普通表转换，但是 reverse 阶段日志中会打印警告【partition tables】，若有要求，需手工转换
   2. 支持自定义配置表字段类型规则转换(table -> schema -> 内置)
   3. 支持默认配置规则转换
2. 支持表索引创建
3. 支持非空约束、外键约束、检查约束等
4. 支持全量数据导出导入
   1. 数据同步导出导入要求表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】

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







