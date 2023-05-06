-- 范围分区
CREATE TABLE orders01 (
                          order_id      NUMBER,
                          order_date    DATE,
                          customer_id   NUMBER,
                          order_total   NUMBER
)
    PARTITION BY RANGE (order_date)
(
   PARTITION orders_q1 VALUES LESS THAN (TO_DATE('01-APR-2022','DD-MON-YYYY')),
   PARTITION orders_q2 VALUES LESS THAN (TO_DATE('01-JUL-2022','DD-MON-YYYY')),
   PARTITION orders_q3 VALUES LESS THAN (TO_DATE('01-OCT-2022','DD-MON-YYYY')),
   PARTITION orders_q4 VALUES LESS THAN (TO_DATE('01-JAN-2023','DD-MON-YYYY'))
);


-- List 分区
CREATE TABLE employees01 (
                             employee_id    NUMBER,
                             first_name     VARCHAR2(50),
                             last_name      VARCHAR2(50),
                             hire_date      DATE,
                             salary         NUMBER,
                             region         VARCHAR2(10)
)
    PARTITION BY LIST (region)
(
   PARTITION east_coast VALUES ('NY', 'NJ', 'CT', 'MA', 'RI', 'NH', 'ME', 'VT'),
   PARTITION west_coast VALUES ('CA', 'OR', 'WA', 'AK', 'HI'),
   PARTITION central VALUES ('IL', 'WI', 'MN', 'IA', 'MO', 'NE', 'KS', 'ND', 'SD')
);

-- HASH 分区
CREATE TABLE sales01 (
                         sale_id     NUMBER,
                         sale_date   DATE,
                         product_id  NUMBER,
                         quantity    NUMBER,
                         price       NUMBER
)
    PARTITION BY HASH (sale_id)
PARTITIONS 128;

-- 复合分区 RANGE-LIST
CREATE TABLE sales02 (
                         product_id    NUMBER,
                         sale_date     DATE,
                         sale_amount   NUMBER
)
    PARTITION BY RANGE (sale_date)
SUBPARTITION BY LIST (product_id)
(
   PARTITION sales_q1_2008 VALUES LESS THAN (TO_DATE('01-APR-2008', 'DD-MON-YYYY'))
   (
      SUBPARTITION sales_q1_east VALUES (1, 3, 5),
      SUBPARTITION sales_q1_west VALUES (2, 4, 6)
   ),
   PARTITION sales_q2_2008 VALUES LESS THAN (TO_DATE('01-JUL-2008', 'DD-MON-YYYY'))
   (
      SUBPARTITION sales_q2_east VALUES (1, 4, 6),
      SUBPARTITION sales_q2_west VALUES (2, 3, 5)
   ),
   PARTITION sales_q3_2008 VALUES LESS THAN (TO_DATE('01-OCT-2008', 'DD-MON-YYYY'))
   (
      SUBPARTITION sales_q3_east VALUES (2, 3, 6),
      SUBPARTITION sales_q3_west VALUES (1, 4, 5)
   ),
   PARTITION sales_q4_2008 VALUES LESS THAN (TO_DATE('01-JAN-2009', 'DD-MON-YYYY'))
   (
      SUBPARTITION sales_q4_east VALUES (1, 4, 5),
      SUBPARTITION sales_q4_west VALUES (2, 3, 6)
   ),
   PARTITION sales_q1_2009 VALUES LESS THAN (TO_DATE('01-APR-2009', 'DD-MON-YYYY'))
   (
      SUBPARTITION sales_q5_east VALUES (2, 3, 5),
      SUBPARTITION sales_q5_west VALUES (1, 4, 6)
   )
);

-- 复合分区 HASH_HASH
CREATE TABLE hash03 (
                                          department_id NUMBER(4) NOT NULL,
                                          department_name VARCHAR2(30),
                                          course_id NUMBER(4) NOT NULL)
    PARTITION BY HASH(department_id)
     SUBPARTITION BY HASH (course_id) SUBPARTITIONS 32 PARTITIONS 16;


-- 复合分区 RANGE-HASH
CREATE TABLE sales03
( prod_id       NUMBER(6)
    , cust_id       NUMBER
    , time_id       DATE
    , channel_id    CHAR(1)
    , promo_id      NUMBER(6)
    , quantity_sold NUMBER(3)
    , amount_sold   NUMBER(10,2)
)
    PARTITION BY RANGE (time_id) INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
 SUBPARTITION BY HASH (cust_id) SUBPARTITIONS 4
 (PARTITION before_2000 VALUES LESS THAN (TO_DATE('01-JAN-2000','dd-MON-yyyy'))
  )
PARALLEL;

-- 复合分区 RANGE-LIST
CREATE TABLE sales_interval_list
( product_id       NUMBER(6)
    , customer_id      NUMBER
    , channel_id       CHAR(1)
    , promo_id         NUMBER(6)
    , sales_date       DATE
    , quantity_sold    INTEGER
    , amount_sold      NUMBER(10,2)
)
    PARTITION BY RANGE (sales_date) INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
 SUBPARTITION BY LIST (channel_id)
   SUBPARTITION TEMPLATE
   ( SUBPARTITION p_catalog VALUES ('C')
   , SUBPARTITION p_internet VALUES ('I')
   , SUBPARTITION p_partners VALUES ('P')
   , SUBPARTITION p_direct_sales VALUES ('S')
   , SUBPARTITION p_tele_sales VALUES ('T')
   )
 (PARTITION before_2017 VALUES LESS THAN (TO_DATE('01-JAN-2017','dd-MON-yyyy'))
  )
PARALLEL;

-- 复合分区 RANGE-RANGE
CREATE TABLE sales04
( prod_id       NUMBER(6)
    , cust_id       NUMBER
    , time_id       DATE
    , channel_id    CHAR(1)
    , promo_id      NUMBER(6)
    , quantity_sold NUMBER(3)
    , amount_sold   NUMBER(10,2)
)
    PARTITION BY RANGE (time_id) INTERVAL (NUMTODSINTERVAL(1,'DAY'))
SUBPARTITION BY RANGE(amount_sold)
   SUBPARTITION TEMPLATE
   ( SUBPARTITION p_low VALUES LESS THAN (1000)
   , SUBPARTITION p_medium VALUES LESS THAN (4000)
   , SUBPARTITION p_high VALUES LESS THAN (8000)
   , SUBPARTITION p_ultimate VALUES LESS THAN (maxvalue)
   )
 (PARTITION before_2000 VALUES LESS THAN (TO_DATE('01-JAN-2000','dd-MON-yyyy'))
  )
PARALLEL;

-- 复合分区 LIST-HASH
CREATE TABLE accounts01
( id             NUMBER
    , account_number NUMBER
    , customer_id    NUMBER
    , balance        NUMBER
    , branch_id      NUMBER
    , region         VARCHAR(2)
    , status         VARCHAR2(1)
)
    PARTITION BY LIST (region)
SUBPARTITION BY HASH (customer_id) SUBPARTITIONS 8
( PARTITION p_northwest VALUES ('OR', 'WA')
 , PARTITION p_southwest VALUES ('AZ', 'UT', 'NM')
 , PARTITION p_northeast VALUES ('NY', 'VM', 'NJ')
 , PARTITION p_southeast VALUES ('FL', 'GA')
 , PARTITION p_northcentral VALUES ('SD', 'WI')
 , PARTITION p_southcentral VALUES ('OK', 'TX')
);

-- 复合分区 LIST-LIST
CREATE TABLE accounts02
( id             NUMBER
    , account_number NUMBER
    , customer_id    NUMBER
    , balance        NUMBER
    , branch_id      NUMBER
    , region         VARCHAR(2)
    , status         VARCHAR2(1)
)
    PARTITION BY LIST (region)
SUBPARTITION BY LIST (status)
( PARTITION p_northwest VALUES ('OR', 'WA')
  ( SUBPARTITION p_nw_bad VALUES ('B')
  , SUBPARTITION p_nw_average VALUES ('A')
  , SUBPARTITION p_nw_good VALUES ('G')
  )
, PARTITION p_southwest VALUES ('AZ', 'UT', 'NM')
  ( SUBPARTITION p_sw_bad VALUES ('B')
  , SUBPARTITION p_sw_average VALUES ('A')
  , SUBPARTITION p_sw_good VALUES ('G')
  )
, PARTITION p_northeast VALUES ('NY', 'VM', 'NJ')
  ( SUBPARTITION p_ne_bad VALUES ('B')
  , SUBPARTITION p_ne_average VALUES ('A')
  , SUBPARTITION p_ne_good VALUES ('G')
  )
, PARTITION p_southeast VALUES ('FL', 'GA')
  ( SUBPARTITION p_se_bad VALUES ('B')
  , SUBPARTITION p_se_average VALUES ('A')
  , SUBPARTITION p_se_good VALUES ('G')
  )
, PARTITION p_northcentral VALUES ('SD', 'WI')
  ( SUBPARTITION p_nc_bad VALUES ('B')
  , SUBPARTITION p_nc_average VALUES ('A')
  , SUBPARTITION p_nc_good VALUES ('G')
  )
, PARTITION p_southcentral VALUES ('OK', 'TX')
  ( SUBPARTITION p_sc_bad VALUES ('B')
  , SUBPARTITION p_sc_average VALUES ('A')
  , SUBPARTITION p_sc_good VALUES ('G')
  )
);

-- 复合分区 LIST-RANGE
CREATE TABLE accounts03
( id             NUMBER
    , account_number NUMBER
    , customer_id    NUMBER
    , balance        NUMBER
    , branch_id      NUMBER
    , region         VARCHAR(2)
    , status         VARCHAR2(1)
)
    PARTITION BY LIST (region)
SUBPARTITION BY RANGE (balance)
( PARTITION p_northwest VALUES ('OR', 'WA')
  ( SUBPARTITION p_nw_low VALUES LESS THAN (1000)
  , SUBPARTITION p_nw_average VALUES LESS THAN (10000)
  , SUBPARTITION p_nw_high VALUES LESS THAN (100000)
  , SUBPARTITION p_nw_extraordinary VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_southwest VALUES ('AZ', 'UT', 'NM')
  ( SUBPARTITION p_sw_low VALUES LESS THAN (1000)
  , SUBPARTITION p_sw_average VALUES LESS THAN (10000)
  , SUBPARTITION p_sw_high VALUES LESS THAN (100000)
  , SUBPARTITION p_sw_extraordinary VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_northeast VALUES ('NY', 'VM', 'NJ')
  ( SUBPARTITION p_ne_low VALUES LESS THAN (1000)
  , SUBPARTITION p_ne_average VALUES LESS THAN (10000)
  , SUBPARTITION p_ne_high VALUES LESS THAN (100000)
  , SUBPARTITION p_ne_extraordinary VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_southeast VALUES ('FL', 'GA')
  ( SUBPARTITION p_se_low VALUES LESS THAN (1000)
  , SUBPARTITION p_se_average VALUES LESS THAN (10000)
  , SUBPARTITION p_se_high VALUES LESS THAN (100000)
  , SUBPARTITION p_se_extraordinary VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_northcentral VALUES ('SD', 'WI')
  ( SUBPARTITION p_nc_low VALUES LESS THAN (1000)
  , SUBPARTITION p_nc_average VALUES LESS THAN (10000)
  , SUBPARTITION p_nc_high VALUES LESS THAN (100000)
  , SUBPARTITION p_nc_extraordinary VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_southcentral VALUES ('OK', 'TX')
  ( SUBPARTITION p_sc_low VALUES LESS THAN (1000)
  , SUBPARTITION p_sc_average VALUES LESS THAN (10000)
  , SUBPARTITION p_sc_high VALUES LESS THAN (100000)
  , SUBPARTITION p_sc_extraordinary VALUES LESS THAN (MAXVALUE)
  )
) ENABLE ROW MOVEMENT;

-- 复合分区 RANG-LIST
CREATE TABLE quarterly_regional_sales
(deptno number, item_no varchar2(20),
 txn_date date, txn_amount number, state varchar2(2))
  PARTITION BY RANGE (txn_date)
    SUBPARTITION BY LIST (state)
      (PARTITION q1_1999 VALUES LESS THAN (TO_DATE('1-APR-1999','DD-MON-YYYY'))
         (SUBPARTITION q1_1999_northwest VALUES ('OR', 'WA'),
          SUBPARTITION q1_1999_southwest VALUES ('AZ', 'UT', 'NM'),
          SUBPARTITION q1_1999_northeast VALUES ('NY', 'VM', 'NJ'),
          SUBPARTITION q1_1999_southeast VALUES ('FL', 'GA'),
          SUBPARTITION q1_1999_northcentral VALUES ('SD', 'WI'),
          SUBPARTITION q1_1999_southcentral VALUES ('OK', 'TX')
         ),
       PARTITION q2_1999 VALUES LESS THAN ( TO_DATE('1-JUL-1999','DD-MON-YYYY'))
         (SUBPARTITION q2_1999_northwest VALUES ('OR', 'WA'),
          SUBPARTITION q2_1999_southwest VALUES ('AZ', 'UT', 'NM'),
          SUBPARTITION q2_1999_northeast VALUES ('NY', 'VM', 'NJ'),
          SUBPARTITION q2_1999_southeast VALUES ('FL', 'GA'),
          SUBPARTITION q2_1999_northcentral VALUES ('SD', 'WI'),
          SUBPARTITION q2_1999_southcentral VALUES ('OK', 'TX')
         ),
       PARTITION q3_1999 VALUES LESS THAN (TO_DATE('1-OCT-1999','DD-MON-YYYY'))
         (SUBPARTITION q3_1999_northwest VALUES ('OR', 'WA'),
          SUBPARTITION q3_1999_southwest VALUES ('AZ', 'UT', 'NM'),
          SUBPARTITION q3_1999_northeast VALUES ('NY', 'VM', 'NJ'),
          SUBPARTITION q3_1999_southeast VALUES ('FL', 'GA'),
          SUBPARTITION q3_1999_northcentral VALUES ('SD', 'WI'),
          SUBPARTITION q3_1999_southcentral VALUES ('OK', 'TX')
         ),
       PARTITION q4_1999 VALUES LESS THAN ( TO_DATE('1-JAN-2000','DD-MON-YYYY'))
         (SUBPARTITION q4_1999_northwest VALUES ('OR', 'WA'),
          SUBPARTITION q4_1999_southwest VALUES ('AZ', 'UT', 'NM'),
          SUBPARTITION q4_1999_northeast VALUES ('NY', 'VM', 'NJ'),
          SUBPARTITION q4_1999_southeast VALUES ('FL', 'GA'),
          SUBPARTITION q4_1999_northcentral VALUES ('SD', 'WI'),
          SUBPARTITION q4_1999_southcentral VALUES ('OK', 'TX')
         )
      );

-- 复合分区 RANGE-RANGE
CREATE TABLE shipments
( order_id      NUMBER NOT NULL
    , order_date    DATE NOT NULL
    , delivery_date DATE NOT NULL
    , customer_id   NUMBER NOT NULL
    , sales_amount  NUMBER NOT NULL
)
    PARTITION BY RANGE (order_date)
SUBPARTITION BY RANGE (delivery_date)
( PARTITION p_2006_jul VALUES LESS THAN (TO_DATE('01-AUG-2006','dd-MON-yyyy'))
  ( SUBPARTITION p06_jul_e VALUES LESS THAN (TO_DATE('15-AUG-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_jul_a VALUES LESS THAN (TO_DATE('01-SEP-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_jul_l VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_2006_aug VALUES LESS THAN (TO_DATE('01-SEP-2006','dd-MON-yyyy'))
  ( SUBPARTITION p06_aug_e VALUES LESS THAN (TO_DATE('15-SEP-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_aug_a VALUES LESS THAN (TO_DATE('01-OCT-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_aug_l VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_2006_sep VALUES LESS THAN (TO_DATE('01-OCT-2006','dd-MON-yyyy'))
  ( SUBPARTITION p06_sep_e VALUES LESS THAN (TO_DATE('15-OCT-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_sep_a VALUES LESS THAN (TO_DATE('01-NOV-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_sep_l VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_2006_oct VALUES LESS THAN (TO_DATE('01-NOV-2006','dd-MON-yyyy'))
  ( SUBPARTITION p06_oct_e VALUES LESS THAN (TO_DATE('15-NOV-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_oct_a VALUES LESS THAN (TO_DATE('01-DEC-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_oct_l VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_2006_nov VALUES LESS THAN (TO_DATE('01-DEC-2006','dd-MON-yyyy'))
  ( SUBPARTITION p06_nov_e VALUES LESS THAN (TO_DATE('15-DEC-2006','dd-MON-yyyy'))
  , SUBPARTITION p06_nov_a VALUES LESS THAN (TO_DATE('01-JAN-2007','dd-MON-yyyy'))
  , SUBPARTITION p06_nov_l VALUES LESS THAN (MAXVALUE)
  )
, PARTITION p_2006_dec VALUES LESS THAN (TO_DATE('01-JAN-2007','dd-MON-yyyy'))
  ( SUBPARTITION p06_dec_e VALUES LESS THAN (TO_DATE('15-JAN-2007','dd-MON-yyyy'))
  , SUBPARTITION p06_dec_a VALUES LESS THAN (TO_DATE('01-FEB-2007','dd-MON-yyyy'))
  , SUBPARTITION p06_dec_l VALUES LESS THAN (MAXVALUE)
  )
);


-- 复合分区 RANGE-HASH
CREATE TABLE employees_sub_template (department_id NUMBER(4) NOT NULL,
                                     last_name VARCHAR2(25), job_id VARCHAR2(10))
    PARTITION BY RANGE(department_id) SUBPARTITION BY HASH(last_name)
     SUBPARTITION TEMPLATE
         (SUBPARTITION a,
          SUBPARTITION b,
          SUBPARTITION c,
          SUBPARTITION d
         )
    (PARTITION p1 VALUES LESS THAN (1000),
     PARTITION p2 VALUES LESS THAN (2000),
     PARTITION p3 VALUES LESS THAN (MAXVALUE)
    );

-- 复合分区 RANGE-LIST
CREATE TABLE stripe_regional_sales
( deptno number, item_no varchar2(20),
  txn_date date, txn_amount number, state varchar2(2))
    PARTITION BY RANGE (txn_date)
   SUBPARTITION BY LIST (state)
   SUBPARTITION TEMPLATE
      (SUBPARTITION northwest VALUES ('OR', 'WA'),
       SUBPARTITION southwest VALUES ('AZ', 'UT', 'NM'),
       SUBPARTITION northeast VALUES ('NY', 'VM', 'NJ'),
       SUBPARTITION southeast VALUES ('FL', 'GA'),
       SUBPARTITION midwest VALUES ('SD', 'WI'),
       SUBPARTITION south VALUES ('AL', 'AK'),
       SUBPARTITION others VALUES (DEFAULT )
      )
  (PARTITION q1_1999 VALUES LESS THAN ( TO_DATE('01-APR-1999','DD-MON-YYYY')),
   PARTITION q2_1999 VALUES LESS THAN ( TO_DATE('01-JUL-1999','DD-MON-YYYY')),
   PARTITION q3_1999 VALUES LESS THAN ( TO_DATE('01-OCT-1999','DD-MON-YYYY')),
   PARTITION q4_1999 VALUES LESS THAN ( TO_DATE('1-JAN-2000','DD-MON-YYYY'))
  );