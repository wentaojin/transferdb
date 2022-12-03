#! /bin/bash
sqlplus marvin/marvin@orcl >>random1.log << EOF
alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss';
create table marvin.marvin3(
ID number primary key,
INC_DATETIME date,
RANDOM_ID number,
RANDOM_STRING varchar2(1000)
);
create index idx_marvin3_RANDOM_ID on marvin.marvin3(RANDOM_ID);

insert into marvin.marvin3
  (ID, INC_DATETIME,RANDOM_ID,RANDOM_STRING)
  select rownum as id,
         to_date(sysdate + rownum / 24 / 3600, 'yyyy-mm-dd hh24:mi:ss') as inc_datetime,
         trunc(dbms_random.value(0, 100)) as random_id,
         dbms_random.string('x', 20) random_string
   from xmltable('1 to 10000000');

create table marvin.marvin4(
ID number primary key,
INC_DATETIME date,
RANDOM_ID number,
RANDOM_STRING varchar2(100)
)
partition by range(ID)
(
   partition par_01 values less than(2000000),
   partition par_02 values less than(4000000),
   partition par_03 values less than(6000000),
   partition par_04 values less than(8000000),
   partition par_05 values less than(maxvalue)
);
create index idx_marvin4_RANDOM_ID on marvin.marvin4(RANDOM_ID);

insert into marvin.marvin4
  (ID, INC_DATETIME,RANDOM_ID,RANDOM_STRING)
  select rownum as id,
         to_date(sysdate + rownum / 24 / 3600, 'yyyy-mm-dd hh24:mi:ss') as inc_datetime,
         trunc(dbms_random.value(0, 100)) as random_id,
         dbms_random.string('x', 20) random_string
   from xmltable('1 to 10000000');
EOF