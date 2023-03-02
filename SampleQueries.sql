
use role accountadmin;

select current_account(), current_user();

use schema snowflake.account_usage;

select * from table (information_schema.warehouse_metering_history(dateadd('days',-10,current_date())));

select * from  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
                     
select * from query_history;

use DATABASE zenas_athleisure_db;

select * from sweatsuits

WITH Averages AS (
    SELECT
        AVG(engineTemperature) averageEngineTemperature,
        AVG(speed) averageSpeed
    FROM
        eventhub TIMESTAMP BY [timestamp]
    GROUP BY
        TumblingWindow(Duration(second, 2))
),
Anomalies AS (
    select
        t.vin,
        t.[timestamp],
        t.city,
        t.region,
        t.outsideTemperature,
        t.engineTemperature,
        a.averageEngineTemperature,
        t.speed,
        a.averageSpeed,
        t.fuel,
        t.engineoil,
        t.tirepressure,
        t.odometer,
        t.accelerator_pedal_position,
        t.parking_brake_status,
        t.headlamp_status,
        t.brake_pedal_status,
        t.transmission_gear_position,
        t.ignition_status,
        t.windshield_wiper_status,
        t.abs,
        (CASE WHEN a.averageEngineTemperature >= 405 OR a.averageEngineTemperature <= 15 THEN 1 ELSE 0 END) AS enginetempanomaly,
        (CASE WHEN t.engineoil <= 1 THEN 1 ELSE 0 END) AS oilanomaly,
        (CASE WHEN (t.transmission_gear_position = 'first' OR
            t.transmission_gear_position = 'second' OR
            t.transmission_gear_position = 'third') AND
            t.brake_pedal_status = 1 AND
            t.accelerator_pedal_position >= 90 AND
            a.averageSpeed >= 55 THEN 1 ELSE 0 END) AS aggressivedriving
    FROM eventhub t TIMESTAMP BY [timestamp]
    INNER JOIN Averages a ON DATEDIFF(second, t, a) BETWEEN 0 And 2
),
VehicleAverages AS (
    SELECT
        AVG(engineTemperature) averageEngineTemperature,
        AVG(speed) averageSpeed,
        System.TimeStamp() AS snapshot
    FROM
        eventhub TIMESTAMP BY [timestamp]
    GROUP BY
        TumblingWindow(Duration(minute, 2))
)

-- INSERT INTO POWER BI
SELECT
    *
INTO
    powerBIAlerts
FROM
    Anomalies
WHERE aggressivedriving = 1 OR enginetempanomaly = 1 OR oilanomaly = 1

-- INSERT INTO SYNAPSE ANALYTICS
SELECT
    *
INTO
    synapse
FROM
    VehicleAverages
