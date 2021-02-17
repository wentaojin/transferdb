### Transferdb

transferdb 用于异构数据库迁移（ Oracle 数据库 -> MySQL 数据库），现阶段支持的功能（原 transferdb 版本被重构，新增自定义转换规则）：

1. 支持表结构定义转换
   1. 支持自定义配置表字段类型规则转换(table -> schema -> 内置)
   2. 支持默认配置规则转换
2. 支持表索引创建
3. 支持非空约束、外键约束、检查约束等

使用事项

```
最好在装有 oracle 环境下 oracle 用户运行
需要把 oracle client instantclient-basic-linux.x64-19.8.0.0.0dbru.zip 上传解压，并配置 LD_LIBRARY_PATH 环境变量
使用方法:

1、上传解压 instantclient-basic-linux.x64-19.8.0.0.0dbru.zip（该压缩包位于 client 目录） 到指定目录，比如：/home/oracle/instantclient_19_8  

2、查看环境变量 LD_LIBRARY_PATH
echo $LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$ORACLE_HOME/rdbms/lib:/lib:/usr/lib:/home/oracle/instantclient_19_8 

3、transferdb 配置文件 config.toml 样例位于 conf 目录下,详情请见说明以及配置
4、$ ./transferdb --config config.toml --mode prepare

元数据库 db_meta（默认）：
表 custom_schema_column_type_maps 库表字段类型转换规则
表 custom_table_column_type_maps 表级别字段类型转换规则，表级别优先级高于库级别
```







