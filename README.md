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
- MySQL/TiDB -> ORACLE 数据库表结构对比【实验性】

Quick Start
-----------
[使用手册](docs/transferdb_guaid.md)

[权限手册](docs/transferdb_privs.md)

[参数说明](example/config.toml)

Development
-----------
环境准备 make prepare

信息评估 make assessO2M/assessO2T

表结构转换 make reverseO2M/reverseO2T reverseM2O/reverseT2O

表结构核对 make checkO2M/checkO2T checkM2O/checkT2O

全量数据迁移 make fullO2M/fullO2T

数据实时同步 make allO2M/allO2T

CSV 数据导出 make csvO2M/csvO2T

数据校验 make compareO2M/compareO2T

程序编译 make build

License
-------
This software is free to use under the Apache License.

