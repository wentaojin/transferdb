-- reverse 验证
/*已知 Oracle 全字段类型*/
create table marvin2(
n1 number primary key,
n2 number(2),
n3 number(4),
n4 number(8),
n5 number(12,0),
n6 number(13),
n7 number(30),
n8 number(30,2),
n9 NUMERIC(10,2),
n10 NUMERIC(10),
nbfile bfile,
vchar1 VARCHAR(10),
vchar2 VARCHAR(3000),
vchar3 VARCHAR2(10),
vchar4 VARCHAR2(3000),
char1 char(23),
char2 char(300),
char3 CHARACTER(23),
char4 CHARACTER(300),
char5 NCHAR(23),
char6 NCHAR(300),
char7 NCHAR VARYING(10),
char8 NCHAR VARYING(300),
char9 NVARCHAR2(10),
char10 NVARCHAR2(300),
dlob CLOB,
cflob NCLOB,
ndate date,
ndecimal1 decimal,
ndecimal2 decimal(10,2),
ndecimal3 DEC(10,2),
ndecimal4 DEC,
dp1 DOUBLE PRECISION,
fp1 FLOAT(2),
fp2 FLOAT,
fy2 INTEGER,
fy4 INT,
fy5 SMALLINT,
yt INTERVAL YEAR(5) TO MONTH,
yu INTERVAL DAY(6) TO SECOND(3),
hp LONG RAW  ,
rw1 RAW(10),
rw2 RAW(300),
rl REAL,
rd1 ROWID,
rd2 UROWID(100),
tp1 TIMESTAMP,
tp2 TIMESTAMP(3),
tp3 TIMESTAMP(5),
tp4 TIMESTAMP(5) WITH TIME ZONE,
xt XMLTYPE
);


/*已知 Oracle 全字段类型*/
create table marvin1(
n1 number primary key,
n2 number(2),
n3 number(4) check(n3 in(1,2 ,3)),
n4 number(8) not null,
n5 number(12,0),
n6 number(13) unique,
n7 number(30),
n8 number(30,2),
n9 NUMERIC(10,2),
n10 NUMERIC(10),
nbfile bfile,
vchar1 VARCHAR(10) default 'ty',
vchar2 VARCHAR(3000),
vchar3 VARCHAR2(10),
vchar4 VARCHAR2(3000),
char1 char(23),
char2 char(300),
char3 CHARACTER(23),
char4 CHARACTER(300),
char5 NCHAR(23),
char6 NCHAR(300),
char7 NCHAR VARYING(10),
char8 NCHAR VARYING(300),
char9 NVARCHAR2(10),
char10 NVARCHAR2(300),
dlob CLOB,
cflob NCLOB,
ndate date,
ndecimal1 decimal,
ndecimal2 decimal(10,2),
ndecimal3 DEC(10,2),
ndecimal4 DEC,
dp1 DOUBLE PRECISION,
fp1 FLOAT(2),
fp2 FLOAT,
fy2 INTEGER,
fy4 INT,
fy5 SMALLINT,
yt INTERVAL YEAR(5) TO MONTH,
yu INTERVAL DAY(6) TO SECOND(3),
flk long,
rw1 RAW(10),
rw2 RAW(300),
rl REAL,
rd1 ROWID,
rd2 UROWID(100),
tp1 TIMESTAMP,
tp2 TIMESTAMP(3),
tp3 TIMESTAMP(5),
tp4 TIMESTAMP(5) WITH TIME ZONE,
xt XMLTYPE,
constraint fk_nam1e foreign key(n2) references marvin2(n1),
constraint unqiue unique (n1, char1)
);

create index idx_marvin1_ty on marvin1(vchar3);
create index idx_marvin1_tu on marvin1(n4,vchar3);

create table marvin3(
n1 number primary key,
n2 number(2),
n3 number(4),
n4 number(8),
n5 number(12,0),
n6 number(13),
n7 number(30),
n8 number(30,2),
n9 NUMERIC(10,2),
n10 NUMERIC(10),
nbfile bfile,
vchar1 VARCHAR(10),
vchar2 VARCHAR(3000),
vchar3 VARCHAR2(10),
vchar4 VARCHAR2(3000),
char1 char,
char2 char(300),
char3 CHARACTER(23),
char4 CHARACTER(300),
char5 NCHAR(23),
char6 NCHAR(300),
char7 NCHAR VARYING(10),
char8 NCHAR VARYING(300),
char9 NVARCHAR2(10),
char10 NVARCHAR2(300),
dlob CLOB,
cflob NCLOB,
ndate date,
ndecimal1 decimal,
ndecimal2 decimal(10,2),
ndecimal3 DEC(10,2),
ndecimal4 DEC,
dp1 DOUBLE PRECISION,
fp1 FLOAT(2),
fp2 FLOAT,
fy2 INTEGER,
fy4 INT,
fy5 SMALLINT,
yt INTERVAL YEAR(5) TO MONTH,
yu INTERVAL DAY(6) TO SECOND(3),
hp LONG RAW  ,
rw1 RAW(10),
rw2 RAW(300),
rl REAL,
rd1 ROWID,
rd2 UROWID(100),
tp1 TIMESTAMP,
tp2 TIMESTAMP(3),
tp3 TIMESTAMP(5),
tp4 TIMESTAMP(5) WITH TIME ZONE,
xt XMLTYPE
);

create table test32( id int primary key,sex varchar(2) check(sex in ('man','woman')),age int, constraint CK_sage1 check(age >1 and age<10));

/*带检查约束表结构*/
create table t_stu(
    stuid      number(10)   primary key,
    stuname    varchar2(20) not null,
    stusex     varchar2(2)  check(stusex in('man','woman'))
);
CREATE TABLE t_stu1 ( stuid NUMBER ( 10, 2 ) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );
CREATE TABLE t_stu2 ( stuid NUMBER ( 11, 0 ) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );
CREATE TABLE t_stu3 ( stuid NUMBER ( 13 ) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );
CREATE TABLE t_stu5 ( stuid NUMBER primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );

CREATE TABLE t_stu4 ( stuid NUMBER(30) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );
CREATE TABLE t_stu6 ( stuid NUMBER(30,2) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );
CREATE TABLE t_stu7 ( stuid NUMBER(22,2) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ) );

CREATE TABLE t_stu8 ( stuid NUMBER(22,2) primary key, stuname VARCHAR2 ( 20 ) NOT NULL, stusex VARCHAR2 ( 2 ),fg long,hk clob );


/*带注释表结构*/
CREATE TABLE  tablename2
(

  ID1            VARCHAR2(50) NOT NULL,
  ID2            INTEGER   NOT NULL,
  SPARE_FIELD_1           NUMBER(28,10),
  SPARE_FIELD_2            NUMBER(28,10),
  SPARE_FIELD_3            NUMBER(28,10),
  UPDATE_TIME              TIMESTAMP(3)
)
COMMENT ON TABLE  tablename
  IS '表tablename';
--字段描述
COMMENT ON COLUMN  tablename.ID1
  IS 'ID1';
COMMENT ON COLUMN  tablename.ID2
  IS 'ID2';
COMMENT ON COLUMN  tablename.SPARE_FIELD_1
  IS '备用字段1';
COMMENT ON COLUMN  tablename.SPARE_FIELD_2
  IS '备用字段2';
COMMENT ON COLUMN  tablename.SPARE_FIELD_3
  IS '备用字段3';
COMMENT ON COLUMN  tablename.UPDATE_TIME
  IS '数据更新时间';

/*自定义转换规则*/
CREATE TABLE marvin.marvin4 (
  id NUMBER primary key,
  name1 VARCHAR2 ( 10 ),
  name2 VARCHAR2 ( 10 ),
  name3 VARCHAR2 ( 10 ),
  loc VARCHAR2 ( 10 ),
  other blob,
  address clob,
  dt timestamp( 9 ),
  d DATE,
  sex NUMBER ( 10 )
);

alter table marvin.marvin4 add constraint cons_uk1 unique(name1);
alter table marvin.marvin4 add constraint cons_uk2 unique(name3,loc);

create unique index uniq_loc on marvin.marvin4(loc);
create unique index uniq_loc_name on marvin.marvin4(loc,name2);

create index idx_name_complex on marvin.marvin4(name2,name3);
create index idx_name_sex_complex on marvin.marvin4(name3,sex);

alter table marvin.marvin4 add constraint cons_uk3 unique(loc,name2);


