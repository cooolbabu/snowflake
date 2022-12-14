
select * from CUSTOMER.SALES.CUSTOMERS;

USE MELS_SMOOTHIE_CHALLENGE_DB;
USE SCHEMA TRAILS;
list @trails_geojson;
list @trails_parquet;
show stages;

select  * 
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');


select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 100;

