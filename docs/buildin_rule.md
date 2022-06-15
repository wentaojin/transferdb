<b>Oracle Data Type Mapping MySQL/TiDB Rule</b>

| ORACLE                             | MySQL/TiDB                   |
|------------------------------------| ---------------------------- |
| number                             | decimal(65,30)               |
| number(p,s)<br />p>0,s>0           | decimal(p,s)                 |
| number(p,0)<br />1<=p<3            | tinyint                      |
| number(p,0)<br />3<=p<5            | smallint                     |
| number(p,0)<br />5<=p<9            | int                          |
| number(p,0)<br />9<=p<19           | bigint                       |
| number(p,0)<br />19<=p<=38         | decimal(p)                   |
| number(p,0)<br />p>38              | decimal(p,4)                 |
| bfile                              | varchar(255)                 |
| char(length)<br />length<256       | char(length)                 |
| char(length)<br />length>=256      | varchar(length)              |
| character(length)<br />length<256  | character(length)            |
| character(length)<br />length>=256 | varchar(length)              |
| clob                               | longtext                     |
| blob                               | blob                         |
| date                               | datetime                     |
| decimal(p,s)<br />p=0,s=0          | decimal                      |
| decimal(p,s)                       | decimal(p,s)                 |
| dec(p,s)<br />p=0,s=0              | decimal                      |
| dec(p,s)                           | decimal(p,s)                 |
| double precision                   | double precision             |
| float(p)<br />p=0                  | float                        |
| float(p)<br />p>0                  | double                       |
| integer                            | int                          |
| int                                | int                          |
| long                               | longtext                     |
| long raw                           | long blob                    |
| binary_float                       | double                       |
| binary_double                      | double                       |
| nchar(length)<br />length<256      | nchar(length)                |
| nchar(length)<br />length>256      | nvarchar(length)             |
| nchar varying                      | nchar varying                |
| nclob                              | text                         |
| numeric(p,s)                       | numeric(p,s)                 |
| nvarchar2(p)                       | nvarchar(p)                  |
| raw(length)                        | varbinary(length)            |
| real                               | double                       |
| rowid                              | char(10)                     |
| smallint                           | decimal(38)                  |
| urowid(length)                     | varchar(length)              |
| varchar2(length)                   | varchar(length)              |
| varchar(length)                    | varchar(length)              |
| xmltype                            | longtext                     |
| interval year(p) to month          | varchar(30)                  |
| interval day(p) to second(s)       | varchar(30)                  |
| timestamp(p)                       | timestamp(p)【精度支持 6位】 |
| timestamp(p) with time zone        | datetime(p)【精度支持 6位】  |
| timestamp(p) with local time zone  | datetime(p)【精度支持 6位】  |
| other data type                    | text                         |



