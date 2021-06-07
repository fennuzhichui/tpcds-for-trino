
create table IF NOT EXISTS icebergtest.iceberg.iceberg_store_sales
USING iceberg
AS SELECT * from tpcds.et_store_sales;

create table IF NOT EXISTS icebergtest.iceberg.iceberg_store_returns
USING iceberg
AS SELECT * from tpcds.et_store_returns;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_catalog_sales
USING iceberg
AS SELECT * from tpcds.et_catalog_sales;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_catalog_returns
    USING iceberg
AS SELECT * from tpcds.et_catalog_returns;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_web_sales
USING iceberg
AS SELECT * from tpcds.et_web_sales;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_web_returns
USING iceberg
AS SELECT * from tpcds.et_web_returns;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_inventory
USING iceberg
AS SELECT * from tpcds.et_inventory;


--创建非事实表
create  table IF NOT EXISTS icebergtest.iceberg.iceberg_store
USING iceberg
AS SELECT * from tpcds.et_store;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_call_center
USING iceberg
AS SELECT * from tpcds.et_call_center;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_catalog_page
USING iceberg
AS SELECT * from tpcds.et_catalog_page;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_web_site
USING iceberg
AS SELECT * from tpcds.et_web_site;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_web_page
USING iceberg
AS SELECT * from tpcds.et_web_page;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_warehouse
USING iceberg
AS SELECT * from tpcds.et_warehouse;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_customer
USING iceberg
AS SELECT * from tpcds.et_customer;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_customer_address
USING iceberg
AS SELECT * from tpcds.et_customer_address;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_customer_demographics
USING iceberg
AS SELECT * from tpcds.et_customer_demographics;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_date_dim
USING iceberg
AS SELECT * from tpcds.et_date_dim;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_household_demographics
USING iceberg
AS SELECT * from tpcds.et_household_demographics;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_item
USING iceberg
AS SELECT * from tpcds.et_item;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_promotion
USING iceberg
AS SELECT * from tpcds.et_promotion;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_reason
USING iceberg
AS SELECT * from tpcds.et_reason;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_ship_mode
USING iceberg
AS SELECT * from tpcds.et_ship_mode;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_time_dim
USING iceberg
AS SELECT * from tpcds.et_time_dim;

create  table IF NOT EXISTS icebergtest.iceberg.iceberg_income_band
USING iceberg
AS SELECT * from tpcds.et_income_band;

show tables;