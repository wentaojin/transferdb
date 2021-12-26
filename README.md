TransferDB
-----------
TransferDB 定位于异构数据库 ORACLE -> MYSQL/TiDB 对象信息收集、表结构映射、表结构对比、数据同步等功能一体化工具

Features
--------
- ORACLE 数据库表结构定义转换，支持库、表、列级别以及默认值自定义
- ORACLE 数据库表索引、非空约束、外键约束、检查约束、主键约束、唯一约束转换
- ORACLE、MySQL/TiDB 数据库表结构对比
- ORACLE 数据库对象信息收集
- ORACLE 数据库全量/CSV 数据迁移
- ORACLE 数据库实时同步【实验性】

Quick Start
-----------
请参考 [使用手册](docs/user_guaid.md)

Development
-----------
信息收集 make gather

表结构转换 make prepare / make reverse

表结构核对 make check

全量数据迁移 make full

数据实时同步 make all

CSV 数据导出 make csv

程序编译 make build

License
-------
This software is free to use under the Apache License.

