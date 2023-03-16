TransferDB
-----------
TransferDB 定位于异构数据库 ORACLE -> MYSQL/TiDB 对象信息收集、表结构映射、表结构对比、数据同步等功能一体化工具

Features
--------
- ORACLE -> MySQL/TiDB 数据库表结构定义转换，支持库、表、列级别以及默认值自定义
- ORACLE -> MySQL/TiDB 数据库表索引、非空约束、外键约束、检查约束、主键约束、唯一约束转换
- ORACLE -> MySQL/TiDB 数据库表结构对比
- ORACLE -> MySQL/TiDB 数据库对象信息收集评估
- ORACLE -> MySQL/TiDB 数据库逻辑数据迁移
- ORACLE -> MySQL/TiDB 数据库CSV数据迁移
- ORACLE -> MySQL/TiDB 数据库数据校验
- ORACLE -> MySQL/TiDB 数据库实时同步【实验性】
- MySQL/TiDB -> ORACLE 数据库表结构定义转换，支持库、表、列级别以及默认值自定义

Quick Start
-----------
[使用手册](docs/transferdb_guaid.md)
[权限手册](docs/transferdb_privs.md)
[参数说明](example/config.toml)

Development
-----------
信息评估 make assess

表结构转换 make prepare / make reverseO2M/reverseM2O

表结构核对 make check

全量数据迁移 make full

数据实时同步 make all

CSV 数据导出 make csv

数据校验 make compare

程序编译 make build

License
-------
This software is free to use under the Apache License.

