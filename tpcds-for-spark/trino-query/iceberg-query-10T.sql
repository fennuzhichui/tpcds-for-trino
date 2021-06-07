-- start query 1 in stream 0 using template query1.tpl
with customer_total_return as
         (select sr_customer_sk as ctr_customer_sk
               ,sr_store_sk as ctr_store_sk
               ,sum(SR_FEE) as ctr_total_return
          from iceberg.iceberg.iceberg_store_returns
             ,iceberg.iceberg.iceberg_date_dim
          where sr_returned_date_sk = d_date_sk
            and d_year =2000
          group by sr_customer_sk
                 ,sr_store_sk)
select  c_customer_id
from customer_total_return ctr1
   ,iceberg.iceberg.iceberg_store
   ,iceberg.iceberg.iceberg_customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'NM'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
    limit 100;


-- start query 1 in stream 0 using template query11.tpl
with year_total as (
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
         ,'s' sale_type
    from iceberg.iceberg.iceberg_customer
       ,iceberg.iceberg.iceberg_store_sales
       ,iceberg.iceberg.iceberg_date_dim
    where c_customer_sk = ss_customer_sk
      and ss_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
    union all
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
         ,'w' sale_type
    from iceberg.iceberg.iceberg_customer
       ,iceberg.iceberg.iceberg_web_sales
       ,iceberg.iceberg.iceberg_date_dim
    where c_customer_sk = ws_bill_customer_sk
      and ws_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
)
select
    t_s_secyear.customer_id
     ,t_s_secyear.customer_first_name
     ,t_s_secyear.customer_last_name
     ,t_s_secyear.customer_email_address
from year_total t_s_firstyear
   ,year_total t_s_secyear
   ,year_total t_w_firstyear
   ,year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 2001
  and t_s_secyear.dyear = 2001+1
  and t_w_firstyear.dyear = 2001
  and t_w_secyear.dyear = 2001+1
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
order by t_s_secyear.customer_id
       ,t_s_secyear.customer_first_name
       ,t_s_secyear.customer_last_name
       ,t_s_secyear.customer_email_address
    limit 100;


-- start query 1 in stream 0 using template query22.tpl
select  i_product_name
     ,i_brand
     ,i_class
     ,i_category
     ,avg(inv_quantity_on_hand) qoh
from iceberg.iceberg.iceberg_inventory
   ,iceberg.iceberg.iceberg_date_dim
   ,iceberg.iceberg.iceberg_item
where inv_date_sk=d_date_sk
  and inv_item_sk=i_item_sk
  and d_month_seq between 1212 and 1212 + 11
group by rollup(i_product_name
       ,i_brand
       ,i_class
       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
    limit 100;


-- start query 1 in stream 0 using template query31.tpl
with ss as
         (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales
          from iceberg.iceberg.iceberg_store_sales,iceberg.iceberg.iceberg_date_dim,iceberg.iceberg.iceberg_customer_address
          where ss_sold_date_sk = d_date_sk
            and ss_addr_sk=ca_address_sk
          group by ca_county,d_qoy, d_year),
     ws as
         (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales
          from iceberg.iceberg.iceberg_web_sales,iceberg.iceberg.iceberg_date_dim,iceberg.iceberg.iceberg_customer_address
          where ws_sold_date_sk = d_date_sk
            and ws_bill_addr_sk=ca_address_sk
          group by ca_county,d_qoy, d_year)
select
    ss1.ca_county
     ,ss1.d_year
     ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
     ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
     ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
     ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
from
    ss ss1
   ,ss ss2
   ,ss ss3
   ,ws ws1
   ,ws ws2
   ,ws ws3
where
        ss1.d_qoy = 1
  and ss1.d_year = 2000
  and ss1.ca_county = ss2.ca_county
  and ss2.d_qoy = 2
  and ss2.d_year = 2000
  and ss2.ca_county = ss3.ca_county
  and ss3.d_qoy = 3
  and ss3.d_year = 2000
  and ss1.ca_county = ws1.ca_county
  and ws1.d_qoy = 1
  and ws1.d_year = 2000
  and ws1.ca_county = ws2.ca_county
  and ws2.d_qoy = 2
  and ws2.d_year = 2000
  and ws1.ca_county = ws3.ca_county
  and ws3.d_qoy = 3
  and ws3.d_year =2000
  and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end
    > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end
  and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end
    > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end
order by ss1.d_year;


-- start query 1 in stream 0 using template query41.tpl
select  distinct(i_product_name)
from iceberg.iceberg.iceberg_item i1
where i_manufact_id between 742 and 742+40
  and (select count(*) as item_cnt
       from iceberg.iceberg.iceberg_item
       where (i_manufact = i1.i_manufact and
              ((i_category = 'Women' and
                (i_color = 'orchid' or i_color = 'papaya') and
                (i_units = 'Pound' or i_units = 'Lb') and
                (i_size = 'petite' or i_size = 'medium')
                   ) or
               (i_category = 'Women' and
                (i_color = 'burlywood' or i_color = 'navy') and
                (i_units = 'Bundle' or i_units = 'Each') and
                (i_size = 'N/A' or i_size = 'extra large')
                   ) or
               (i_category = 'Men' and
                (i_color = 'bisque' or i_color = 'azure') and
                (i_units = 'N/A' or i_units = 'Tsp') and
                (i_size = 'small' or i_size = 'large')
                   ) or
               (i_category = 'Men' and
                (i_color = 'chocolate' or i_color = 'cornflower') and
                (i_units = 'Bunch' or i_units = 'Gross') and
                (i_size = 'petite' or i_size = 'medium')
                   ))) or
           (i_manufact = i1.i_manufact and
            ((i_category = 'Women' and
              (i_color = 'salmon' or i_color = 'midnight') and
              (i_units = 'Oz' or i_units = 'Box') and
              (i_size = 'petite' or i_size = 'medium')
                 ) or
             (i_category = 'Women' and
              (i_color = 'snow' or i_color = 'steel') and
              (i_units = 'Carton' or i_units = 'Tbl') and
              (i_size = 'N/A' or i_size = 'extra large')
                 ) or
             (i_category = 'Men' and
              (i_color = 'purple' or i_color = 'gainsboro') and
              (i_units = 'Dram' or i_units = 'Unknown') and
              (i_size = 'small' or i_size = 'large')
                 ) or
             (i_category = 'Men' and
              (i_color = 'metallic' or i_color = 'forest') and
              (i_units = 'Gram' or i_units = 'Ounce') and
              (i_size = 'petite' or i_size = 'medium')
                 )))) > 0
order by i_product_name
    limit 100;


-- start query 1 in stream 0 using template query51.tpl
WITH web_v1 as (
    select
        ws_item_sk item_sk, d_date,
        sum(sum(ws_sales_price))
                   over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
    from iceberg.iceberg.iceberg_web_sales
       ,iceberg.iceberg.iceberg_date_dim
    where ws_sold_date_sk=d_date_sk
      and d_month_seq between 1212 and 1212+11
      and ws_item_sk is not NULL
    group by ws_item_sk, d_date),
     store_v1 as (
         select
             ss_item_sk item_sk, d_date,
             sum(sum(ss_sales_price))
                        over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
         from iceberg.iceberg.iceberg_store_sales
            ,iceberg.iceberg.iceberg_date_dim
         where ss_sold_date_sk=d_date_sk
           and d_month_seq between 1212 and 1212+11
           and ss_item_sk is not NULL
         group by ss_item_sk, d_date)
select  *
from (select item_sk
           ,d_date
           ,web_sales
           ,store_sales
           ,max(web_sales)
        over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max(store_sales)
            over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
      from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
            from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
                and web.d_date = store.d_date)
           )x )y
where web_cumulative > store_cumulative
order by item_sk
       ,d_date
    limit 100;


-- start query 1 in stream 0 using template query60.tpl
with ss as (
    select
        i_item_id,sum(ss_ext_sales_price) total_sales
    from
        iceberg.iceberg.iceberg_store_sales,
        iceberg.iceberg.iceberg_date_dim,
        iceberg.iceberg.iceberg_customer_address,
        iceberg.iceberg.iceberg_item
    where
            i_item_id in (select
                              i_item_id
                          from
                              iceberg.iceberg.iceberg_item
                          where i_category in ('Children'))
      and     ss_item_sk              = i_item_sk
      and     ss_sold_date_sk         = d_date_sk
      and     d_year                  = 1999
      and     d_moy                   = 9
      and     ss_addr_sk              = ca_address_sk
      and     ca_gmt_offset           = -6
    group by i_item_id),
     cs as (
         select
             i_item_id,sum(cs_ext_sales_price) total_sales
         from
             iceberg.iceberg.iceberg_catalog_sales,
             iceberg.iceberg.iceberg_date_dim,
             iceberg.iceberg.iceberg_customer_address,
             iceberg.iceberg.iceberg_item
         where
                 i_item_id               in (select
                                                 i_item_id
                                             from
                                                 iceberg.iceberg.iceberg_item
                                             where i_category in ('Children'))
           and     cs_item_sk              = i_item_sk
           and     cs_sold_date_sk         = d_date_sk
           and     d_year                  = 1999
           and     d_moy                   = 9
           and     cs_bill_addr_sk         = ca_address_sk
           and     ca_gmt_offset           = -6
         group by i_item_id),
     ws as (
         select
             i_item_id,sum(ws_ext_sales_price) total_sales
         from
             iceberg.iceberg.iceberg_web_sales,
             iceberg.iceberg.iceberg_date_dim,
             iceberg.iceberg.iceberg_customer_address,
             iceberg.iceberg.iceberg_item
         where
                 i_item_id               in (select
                                                 i_item_id
                                             from
                                                 iceberg.iceberg.iceberg_item
                                             where i_category in ('Children'))
           and     ws_item_sk              = i_item_sk
           and     ws_sold_date_sk         = d_date_sk
           and     d_year                  = 1999
           and     d_moy                   = 9
           and     ws_bill_addr_sk         = ca_address_sk
           and     ca_gmt_offset           = -6
         group by i_item_id)
select
    i_item_id
     ,sum(total_sales) total_sales
from  (select * from ss
       union all
       select * from cs
       union all
       select * from ws) tmp1
group by i_item_id
order by i_item_id
       ,total_sales
    limit 100;



-- start query 1 in stream 0 using template query71.tpl
select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
       sum(ext_price) ext_price
from iceberg.iceberg.iceberg_item, (select ws_ext_sales_price as ext_price,
                   ws_sold_date_sk as sold_date_sk,
                   ws_item_sk as sold_item_sk,
                   ws_sold_time_sk as time_sk
            from iceberg.iceberg.iceberg_web_sales,iceberg.iceberg.iceberg_date_dim
            where d_date_sk = ws_sold_date_sk
              and d_moy=12
              and d_year=2000
            union all
            select cs_ext_sales_price as ext_price,
                   cs_sold_date_sk as sold_date_sk,
                   cs_item_sk as sold_item_sk,
                   cs_sold_time_sk as time_sk
            from iceberg.iceberg.iceberg_catalog_sales,iceberg.iceberg.iceberg_date_dim
            where d_date_sk = cs_sold_date_sk
              and d_moy=12
              and d_year=2000
            union all
            select ss_ext_sales_price as ext_price,
                   ss_sold_date_sk as sold_date_sk,
                   ss_item_sk as sold_item_sk,
                   ss_sold_time_sk as time_sk
            from iceberg.iceberg.iceberg_store_sales,iceberg.iceberg.iceberg_date_dim
            where d_date_sk = ss_sold_date_sk
              and d_moy=12
              and d_year=2000
) tmp,iceberg.iceberg.iceberg_time_dim
where
        sold_item_sk = i_item_sk
  and i_manager_id=1
  and time_sk = t_time_sk
  and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
group by i_brand, i_brand_id,t_hour,t_minute
order by ext_price desc, i_brand_id
;


-- start query 1 in stream 0 using template query81.tpl
with customer_total_return as
         (select cr_returning_customer_sk as ctr_customer_sk
                  ,ca_state as ctr_state,
                 sum(cr_return_amt_inc_tax) as ctr_total_return
          from iceberg.iceberg.iceberg_catalog_returns
             ,iceberg.iceberg.iceberg_date_dim
             ,iceberg.iceberg.iceberg_customer_address
          where cr_returned_date_sk = d_date_sk
            and d_year =1998
            and cr_returning_addr_sk = ca_address_sk
          group by cr_returning_customer_sk
                 ,ca_state )
select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
     ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
     ,ca_location_type,ctr_total_return
from customer_total_return ctr1
   ,iceberg.iceberg.iceberg_customer_address
   ,iceberg.iceberg.iceberg_customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_state = ctr2.ctr_state)
  and ca_address_sk = c_current_addr_sk
  and ca_state = 'IL'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
       ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
       ,ca_location_type,ctr_total_return
    limit 100;


-- start query 1 in stream 0 using template query91.tpl
select
    cc_call_center_id Call_Center,
    cc_name Call_Center_Name,
    cc_manager Manager,
    sum(cr_net_loss) Returns_Loss
from
    iceberg.iceberg.iceberg_call_center,
    iceberg.iceberg.iceberg_catalog_returns,
    iceberg.iceberg.iceberg_date_dim,
    iceberg.iceberg.iceberg_customer,
    iceberg.iceberg.iceberg_customer_address,
    iceberg.iceberg.iceberg_customer_demographics,
    iceberg.iceberg.iceberg_household_demographics
where
        cr_call_center_sk       = cc_call_center_sk
  and     cr_returned_date_sk     = d_date_sk
  and     cr_returning_customer_sk= c_customer_sk
  and     cd_demo_sk              = c_current_cdemo_sk
  and     hd_demo_sk              = c_current_hdemo_sk
  and     ca_address_sk           = c_current_addr_sk
  and     d_year                  = 1999
  and     d_moy                   = 11
  and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
    or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
  and     hd_buy_potential like '0-500%'
  and     ca_gmt_offset           = -7
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by sum(cr_net_loss) desc;



