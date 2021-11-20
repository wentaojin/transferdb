/*
 oracle partition table maybe mysql has compatibility, will convert to normal table, please manual adjust
┌────────┬───────────────────────┬────────────────────────────────┐
│ SCHEMA │ ORACLE PARTITION LIST │ SUGGEST                        │
├────────┼───────────────────────┼────────────────────────────────┤
│ marvin │ list_partition_table  │ Manual Create And Adjust Table │
│        │ hash_rang             │                                │
│        │ gprs_celltopvol_wk    │                                │
│        │ mobilemessage         │                                │
└────────┴───────────────────────┴────────────────────────────────┘
*/
/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬────────────────────┬────────────────────┬───────────────┐
│ #     │ ORACLE             │ MYSQL              │ SUGGEST       │
├───────┼────────────────────┼────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_STOCK │ steven.BMSQL_STOCK │ Manual Create │
└───────┴────────────────────┴────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_STOCK ADD CONSTRAINT `s_item_fkey` FOREIGN KEY(s_i_id) REFERENCES `marvin`.`bmsql_item`(i_id);
ALTER TABLE steven.BMSQL_STOCK ADD CONSTRAINT `s_warehouse_fkey` FOREIGN KEY(s_w_id) REFERENCES `marvin`.`bmsql_warehouse`(w_id);

-- the above info comes from oracle table [marvin.BMSQL_STOCK]
-- the above info create mysql table sql [steven.BMSQL_STOCK]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬─────────────────────────┬─────────────────────────┬───────────────┐
│ #     │ ORACLE                  │ MYSQL                   │ SUGGEST       │
├───────┼─────────────────────────┼─────────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_ORDER_LINE │ steven.BMSQL_ORDER_LINE │ Manual Create │
└───────┴─────────────────────────┴─────────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_ORDER_LINE ADD CONSTRAINT `ol_order_fkey` FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES `marvin`.`bmsql_oorder`(o_w_id,o_d_id,o_id);
ALTER TABLE steven.BMSQL_ORDER_LINE ADD CONSTRAINT `ol_stock_fkey` FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES `marvin`.`bmsql_stock`(s_w_id,s_i_id);

-- the above info comes from oracle table [marvin.BMSQL_ORDER_LINE]
-- the above info create mysql table sql [steven.BMSQL_ORDER_LINE]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬─────────────────────┬─────────────────────┬───────────────┐
│ #     │ ORACLE              │ MYSQL               │ SUGGEST       │
├───────┼─────────────────────┼─────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_OORDER │ steven.BMSQL_OORDER │ Manual Create │
└───────┴─────────────────────┴─────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_OORDER ADD CONSTRAINT `o_customer_fkey` FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES `marvin`.`bmsql_customer`(c_w_id,c_d_id,c_id);

-- the above info comes from oracle table [marvin.BMSQL_OORDER]
-- the above info create mysql table sql [steven.BMSQL_OORDER]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬────────────────────────┬────────────────────────┬───────────────┐
│ #     │ ORACLE                 │ MYSQL                  │ SUGGEST       │
├───────┼────────────────────────┼────────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_NEW_ORDER │ steven.BMSQL_NEW_ORDER │ Manual Create │
└───────┴────────────────────────┴────────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_NEW_ORDER ADD CONSTRAINT `no_order_fkey` FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES `marvin`.`bmsql_oorder`(o_w_id,o_d_id,o_id);

-- the above info comes from oracle table [marvin.BMSQL_NEW_ORDER]
-- the above info create mysql table sql [steven.BMSQL_NEW_ORDER]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬──────────────────────┬──────────────────────┬───────────────┐
│ #     │ ORACLE               │ MYSQL                │ SUGGEST       │
├───────┼──────────────────────┼──────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_HISTORY │ steven.BMSQL_HISTORY │ Manual Create │
└───────┴──────────────────────┴──────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_HISTORY ADD CONSTRAINT `h_customer_fkey` FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES `marvin`.`bmsql_customer`(c_w_id,c_d_id,c_id);
ALTER TABLE steven.BMSQL_HISTORY ADD CONSTRAINT `h_district_fkey` FOREIGN KEY(h_w_id,h_d_id) REFERENCES `marvin`.`bmsql_district`(d_w_id,d_id);

-- the above info comes from oracle table [marvin.BMSQL_HISTORY]
-- the above info create mysql table sql [steven.BMSQL_HISTORY]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬───────────────────────┬───────────────────────┬───────────────┐
│ #     │ ORACLE                │ MYSQL                 │ SUGGEST       │
├───────┼───────────────────────┼───────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_CUSTOMER │ steven.BMSQL_CUSTOMER │ Manual Create │
└───────┴───────────────────────┴───────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_CUSTOMER ADD CONSTRAINT `c_district_fkey` FOREIGN KEY(c_w_id,c_d_id) REFERENCES `marvin`.`bmsql_district`(d_w_id,d_id);

-- the above info comes from oracle table [marvin.BMSQL_CUSTOMER]
-- the above info create mysql table sql [steven.BMSQL_CUSTOMER]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬───────────────────────┬───────────────────────┬───────────────┐
│ #     │ ORACLE                │ MYSQL                 │ SUGGEST       │
├───────┼───────────────────────┼───────────────────────┼───────────────┤
│ TABLE │ marvin.BMSQL_DISTRICT │ steven.BMSQL_DISTRICT │ Manual Create │
└───────┴───────────────────────┴───────────────────────┴───────────────┘
*/
ALTER TABLE steven.BMSQL_DISTRICT ADD CONSTRAINT `d_warehouse_fkey` FOREIGN KEY(d_w_id) REFERENCES `marvin`.`bmsql_warehouse`(w_id);

-- the above info comes from oracle table [marvin.BMSQL_DISTRICT]
-- the above info create mysql table sql [steven.BMSQL_DISTRICT]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬──────────────────┬──────────────────┬───────────────┐
│ #     │ ORACLE           │ MYSQL            │ SUGGEST       │
├───────┼──────────────────┼──────────────────┼───────────────┤
│ TABLE │ marvin.NEWAUTHOR │ steven.NEWAUTHOR │ Manual Create │
└───────┴──────────────────┴──────────────────┴───────────────┘
*/
ALTER TABLE steven.NEWAUTHOR ADD CONSTRAINT `sys_c0010039` FOREIGN KEY(book_id) REFERENCES `marvin`.`newbook_mast`(book_id);
ALTER TABLE steven.NEWAUTHOR ADD CONSTRAINT `sys_c0010040` FOREIGN KEY(book_id1) REFERENCES `marvin`.`newbook_mast`(book_id) ON DELETE CASCADE;
ALTER TABLE steven.NEWAUTHOR ADD CONSTRAINT `sys_c0010041` FOREIGN KEY(book_id2) REFERENCES `marvin`.`newbook_mast`(book_id) ON DELETE SET NULL;
ALTER TABLE steven.NEWAUTHOR ADD CONSTRAINT `sys_c0010036` CHECK (country IN ('USA', 'UK', 'India'));

-- the above info comes from oracle table [marvin.NEWAUTHOR]
-- the above info create mysql table sql [steven.NEWAUTHOR]

/*
 oracle table check consrtaint maybe mysql has compatibility, skip
┌───────┬────────────────────┬────────────────────┬───────────────┐
│ #     │ ORACLE             │ MYSQL              │ SUGGEST       │
├───────┼────────────────────┼────────────────────┼───────────────┤
│ TABLE │ marvin.UNIQUE_TEST │ steven.UNIQUE_TEST │ Manual Create │
└───────┴────────────────────┴────────────────────┴───────────────┘
*/
CREATE INDEX `idx_po` ON `steven`.`UNIQUE_TEST`(SUBSTR("FNAME",1,3));

-- the above info comes from oracle table [marvin.UNIQUE_TEST]
-- the above info create mysql table sql [steven.UNIQUE_TEST]

