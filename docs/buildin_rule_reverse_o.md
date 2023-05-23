<b>Oracle Data Type Mapping MySQL/TiDB Rule</b>

| ORACLE                            | MySQL/TiDB          |
|-----------------------------------|---------------------|
| number                            | decimal(65,30)      |
| number(*)                         | decimal(65,30)      |
| number(*,s)<br />0<s<=30          | decimal(65,s)       |
| number(*,s)<br />s>30             | decimal(65,30)      |
| number(p,s)<br />p>0,0<s<=30      | decimal(p,s)        |
| number(p,s)<br />p>0,s>30         | decimal(p,30)       |
| number(p,0)<br />1<=p<3           | tinyint             |
| number(p,0)<br />3<=p<5           | smallint            |
| number(p,0)<br />5<=p<9           | int                 |
| number(p,0)<br />9<=p<19          | bigint              |
| number(p,0)<br />19<=p<=38        | decimal(p)          |
| number(p,0)<br />p>38             | decimal(65)         |
| bfile                             | varchar(255)        |
| char(length)                      | varchar(length)     |
| character(length)                 | varchar(length)     |
| clob                              | longtext            |
| blob                              | blob                |
| date                              | datetime            |
| decimal(p,s)                      | decimal(p,s)        |
| dec(p,s)                          | decimal(p,s)        |
| double precision                  | double precision    |
| float(p)                          | double              |
| integer                           | int                 |
| int                               | int                 |
| long                              | longtext            |
| long raw                          | long blob           |
| binary_float                      | double              |
| binary_double                     | double              |
| nchar(length)                     | varchar(length)     |
| nchar varying                     | nchar varying       |
| nclob                             | text                |
| numeric(p,s)                      | numeric(p,s)        |
| nvarchar2(p)                      | varchar(p)          |
| raw(length)                       | varbinary(length)   |
| real                              | double              |
| rowid                             | char(10)            |
| smallint                          | smallint            |
| urowid(length)                    | varchar(length)     |
| varchar2(length)                  | varchar(length)     |
| varchar(length)                   | varchar(length)     |
| xmltype                           | longtext            |
| interval year(p) to month         | varchar(30)         |
| interval day(p) to second(s)      | varchar(30)         |
| timestamp(p)                      | timestamp(p)【精度支持 6位】 |
| timestamp(p) with time zone       | datetime(p)【精度支持 6位】 |
| timestamp(p) with local time zone | datetime(p)【精度支持 6位】 |
| other data type                   | text                |