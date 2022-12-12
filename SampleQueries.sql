
use role accountadmin;

select current_account(), current_user();



use schema snowflake.account_usage;

select * from table (information_schema.warehouse_metering_history(dateadd('days',-10,current_date())));

select * from  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
                     
select * from query_history;