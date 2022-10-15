<b>MySQL/TiDB Data Type Mapping ORACLE Rule</b>

| MySQL/TiDB                         | ORACLE                |
|------------------------------------|-----------------------|
| bigint(P)                          | number(19,0)          |
| decimal(M,D)                       | number                |
| double(P,S)                        | binary_double         |
| double precision                   | binary_double         |
| float(P,S)                         | binary_float          |
| int(P)                             | number(10,0)          |
| integer(P)                         | number(10,0)          |
| mediumint(P)                       | number(7,0)           |
| numeric(M,D)                       | number                |
| real                               | binary_float          |
| smallint(P)                        | number(5,0)           |
| tinyint(P)                         | number(3,0)           |
| bit(P)                             | raw(P)                |
| date                               | date                  |
| datetime(X)                        | date                  |
| timestamp(X)                       | timestamp(X)          |
| time(X)                            | date                  |
| year                               | number                |
| blob                               | blob                  |
| char(M)                            | char(M)               |
| longblob                           | blob                  |
| longtext                           | clob                  |
| mediumblob                         | blob                  |
| mediumtext                         | clob                  |
| text                               | clob                  |
| tinyblob                           | blob                  |
| tinytext                           | varchar2(M)           |
| varchar(M)                         | varchar2(M)           |
| set                                | not support           |
| enum                               | not support           |